# Dual Walk + Momentum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed-leg walk-mode evaluator with an optimal dual-pointer depth walk, direction selection from BBO improvement, three depth-risk knobs, and a momentum trade mode that takes priority over arbitrage.

**Architecture:** `sync_bbo` gains improvement-direction flags; `evaluate_arbitrage` picks one direction per event and calls a new greedy `dual_walk` planner; a new `momentum` module gates and builds directional round-trip orders that jump the queue ahead of arb; the trade channel carries a `TradeCommand` enum so `trade.rs` can execute either an arb pair-trade or a momentum round trip.

**Tech Stack:** Rust (tokio, tokio-tungstenite, serde, influx_db_client, rand). Verify with `cargo test` and `cargo clippy --all-targets`.

**Spec:** `docs/superpowers/specs/2026-07-18-dual-walk-momentum-design.md`

## Global Constraints

- `BOOK_DEPTH = 10` (unchanged, `src/orderbook.rs`).
- Defaults: `MAX_WALK_DEPTH = 10`, `DEPTH_HAIRCUT_PCT = 70`, `ROI_BUFFER_BPS = 2`, momentum off.
- `CONVERGENCE_FRACTION = 0.5` constant in `src/momentum.rs`.
- Hold time: `hold_ms = 10^uniform(0,3)` ms (log-uniform 1–1000), logged.
- Momentum has priority over arb on `bid_improved` events when enabled.
- Telemetry tag values: `bid_improved`, `ask_improved` (tag key `trigger`).
- Hot path must never block: telemetry via `try_log` / spawned Influx writes only.

---

### Task 1: BboChange improvement flags

**Files:**
- Modify: `src/orderbook.rs` (struct `BboChange`, `sync_bbo`)

**Interfaces:**
- Produces: `BboChange { changed, bid_price_changed, ask_price_changed, bid_volume_changed, ask_volume_changed, bid_improved: bool, ask_improved: bool }`. `ask_changed()` / `bid_changed()` removed.

- [ ] Add `bid_improved` / `ask_improved` fields; compute in `sync_bbo` before overwriting `pair_data`:
  `bid_improved = new_bid > old_bid || (new_bid == old_bid && new_bid_vol > old_bid_vol)`;
  `ask_improved = (new_ask < old_ask && new_ask > 0) || (new_ask == old_ask && new_ask_vol > old_ask_vol)`.
  Guard the initial-sync case (`old == 0.0`) as an improvement so the first snapshot can trigger evaluation.
- [ ] Remove `ask_changed()` / `bid_changed()`.
- [ ] Tests in `orderbook.rs`: price up/down on each side, volume up at same price, worsening moves set neither, first sync sets both.
- [ ] `cargo test` passes for the module; commit.

### Task 2: Risk knobs and CLI flags

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Produces: `pub static MAX_WALK_DEPTH: AtomicI16` (10), `pub static DEPTH_HAIRCUT_PCT: AtomicI16` (70), `pub static ROI_BUFFER_BPS: AtomicI16` (2), `pub static MOMENTUM_ENABLED: AtomicBool` (false).

- [ ] Add statics beside `FEE_SPOT`; parse `--max-walk-depth N`, `--depth-haircut N`, `--roi-buffer-bps N`, `--momentum` in `Config::initialize` (value-style args parse the following token); store into statics at startup and log the effective values.
- [ ] Commit.

### Task 3: TradeCommand and MomentumOrder types

**Files:**
- Modify: `src/structs.rs`

**Interfaces:**
- Produces:
  `pub enum TradeCommand { Arb(OrderInfo), Momentum(MomentumOrder) }`;
  `pub struct MomentumOrder { pair_name, trigger_pair_name: &'static str, volume_coin, volume_decimals_coin, limit_buy_price, price_decimals, hold_ms: f64, gap_bps: f64, send_timestamp: u128, updated_pair_kraken_ts: f64, opportunity_id: u64 }`.

- [ ] Add types; change channel type usages in `main.rs`, `threads.rs`, `listener.rs` from `mpsc::Sender<OrderInfo>` to `mpsc::Sender<TradeCommand>` (and receiver in `trade.rs` signature — body updated in Task 6).
- [ ] Commit (compiles once Tasks 4–6 land; keep on a branch commit if intermediate build breaks are unacceptable, otherwise fold into Task 4 commit).

### Task 4: Dual-walk planner + direction selection

**Files:**
- Modify: `src/evaluate_arbitrage.rs`

**Interfaces:**
- Consumes: `BboChange.bid_improved/ask_improved`, risk statics from Task 2.
- Produces: `enum Trigger { BidImproved, AskImproved }` with `as_str()`; `fn dual_walk(book_buy, book_sell, pair1_stable, pair2_stable, balance, fee_spot, arb_fee, max_depth, haircut, roi_buffer) -> Option<DepthFill>`; `DepthFill.trigger: Trigger` (replaces `walk_mode`).

- [ ] Delete `WalkMode`, `resolve_walk_mode`, `fixed_ask_walk_bids`, `fixed_bid_walk_asks`, `compute_depth_fill`.
- [ ] Implement `dual_walk` per spec section 3 (two pointers, ROI gate `> 1 + buffer`, haircut beyond L0, depth cap, balance cap, slices for both legs, `limit_buy_price` = deepest ask touched, stop reasons `unprofitable_level` / `balance_capped` / `book_depth_exhausted` / `depth_cap_reached`). Slice cap = `2 * BOOK_DEPTH`.
- [ ] Rewrite `evaluate_arbitrage`: identify X (updated pair) and sibling Y; on `bid_improved` evaluate buy-Y-sell-X (momentum first — Task 5); on `ask_improved` evaluate buy-X-sell-Y; both flags → both directions. Keep top-of-book ROI pre-filter at `1 + buffer`.
- [ ] `process_arbitrage_opportunity` takes `Trigger`, calls `dual_walk`, sends `TradeCommand::Arb`.
- [ ] Tests: dual-level crossing both sides, haircut, depth cap, ROI buffer, balance cap, stop reasons, VWAP/PnL math, direction selection. Remove old walk-mode tests.
- [ ] `cargo test`; commit.

### Task 5: Momentum entry gate

**Files:**
- Create: `src/momentum.rs`
- Modify: `src/main.rs` (mod), `src/evaluate_arbitrage.rs` (call site)

**Interfaces:**
- Produces: `pub fn maybe_momentum_order(x_pair, y_pair, x_stable, y_stable, y_pair_name, x_pair_name, balance, fee_spot, kraken_ts) -> Option<MomentumOrder>`; `pub fn sample_hold_ms() -> f64`.

- [ ] Implement gate per spec section 5 (fx conversion, gap > 0, `exit_est` fee coverage, sizing = min(top ask vol, balance headroom), guardrails via `order_min`/`cost_min`, hold-time sample).
- [ ] Call from `evaluate_arbitrage` on `bid_improved` when `MOMENTUM_ENABLED`; if an order is produced and sent, skip arb for that event.
- [ ] Tests: gate boundary (exactly covers fees), negative gap, balance sizing, hold range over many samples.
- [ ] `cargo test`; commit.

### Task 6: Momentum execution path

**Files:**
- Modify: `src/trade.rs`

**Interfaces:**
- Consumes: `TradeCommand` from channel; `MomentumOrder`.
- Produces: `make_momentum_trade(...)` — limit IOC buy at `limit_buy_price`, collect fills (existing ownTrades machinery), `sleep(hold_ms)`, market sell filled volume on the same pair, forensics + Influx.

- [ ] Match on `TradeCommand` in `run_trading_thread`; reuse staleness gate for both variants.
- [ ] Implement momentum execution; realized PnL same-currency: `sell_proceeds − sell_fee − buy_cost − buy_fee`.
- [ ] Commit.

### Task 7: Telemetry rename + momentum telemetry

**Files:**
- Modify: `src/arb_forensics.rs`, `src/influx.rs`, `src/evaluate_arbitrage.rs`, `src/trade.rs`

- [ ] Rename `walk_mode` → `trigger` in `ArbOpportunityEvent` and `log_arbitrage_opportunity` (tag key `trigger`).
- [ ] Add `MomentumExecutionEvent` forensics struct + `ForensicsEvent::MomentumExecution` variant; add `log_momentum_execution` Influx writer (measurement `momentum_execution`, tags: pair, trigger_pair; fields: gap_bps, hold_time_ms, requested/filled volumes, entry/exit VWAPs, fees, realized_pnl, outcome).
- [ ] Commit.

### Task 8: Docs and scripts

**Files:**
- Modify: `README.md`, `docs/chronograf-monitoring.md`, `scripts/analyze_arb_events.py`

- [ ] Rename `walk_mode` → `trigger` everywhere (InfluxQL `GROUP BY`, tag tables, funnel notes); document new CLI flags and the `momentum_execution` measurement.
- [ ] Commit.

### Task 9: Full verification

- [ ] `cargo test` — all green.
- [ ] `cargo clippy --all-targets` — no warnings.
- [ ] Commit any fixes.
