# Maker Mode on Thin Books — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship an MVP `--maker` mode that posts/cancels small resting post-only LIMIT quotes on a crypto pair using sibling-pair FX fair value, with hard notional caps and JSONL forensics — without building a full market-making platform.

**Architecture:** Pure quote logic lives in a new `maker` module (fair mid from sibling BBO + FX, inside-spread prices, inventory gates). On each BBO update when `--maker` is on, `evaluate_arbitrage` skips arb/momentum and emits `TradeCommand::Maker`. The trading thread reconciles desired vs resting quotes via WS `addOrder` (GTC + `post`) / `cancelOrder` (by `userref`), tracks inventory from `ownTrades`, and never uses market sells as the primary exit. Cancel-before-pickoff is the edge: maker actions do not hold `TRADER_BUSY` across fill waits.

**Tech Stack:** Rust (tokio, tokio-tungstenite, serde), existing forensics JSONL + private WS patterns.

## Global Constraints

- ONE primary flag `--maker` plus at most two knobs: `--maker-notional N` (quote currency dollars, default `10`), `--maker-offset-bps N` (lean from fair mid, default `5`).
- Off by default. When `--maker` is on, **arbitrage and momentum are disabled** (maker takes exclusive control of the trade channel). Document in README.
- Live posts still require `--trade` (same as arb). Without `--trade`, decisions are forensics-only.
- Maker fee assumption: constant `FEE_MAKER_BPS = 16` (Kraken mid-tier maker). `FEE_SPOT` remains taker and is not used for maker quote placement. Document; do not wire `fees_maker` from TradeVolume in MVP.
- Hard inventory notional cap ≈ `--maker-notional` (stranded risk bounded ~$5–10 style).
- No market sells as primary exit; exit via cancel + opposite maker limit (or leave inventory until opposite fill / manual).
- YAGNI: no multi-venue, no inventory skew optimizer, no multi-level ladder — one bid + one ask per pair max.
- Hot path must never block: forensics via `try_log` only.
- `cargo test` must pass; TDD for pure quote math.

## MVP includes vs deferred

| Included (MVP) | Deferred |
|---|---|
| `--maker` + notional + offset knobs | Multi-level quotes / ladders |
| Sibling FX fair mid | Fancy inventory skew / vol targeting |
| Post-only GTC bid+ask inside spread | OpenOrders WS reconciliation |
| Aggressive cancel/replace on adverse fair move | Maker fee fetch from TradeVolume |
| Inventory notional cap + one-sided quoting when capped | Soft exit via careful limit sweep beyond maker |
| Forensics: quote / cancel / fill / inventory | Influx panels for maker |
| Arb+momentum disabled when maker on | Quoting many pairs with global risk model |
| README run + risks | Full MM platform / multi-venue |

## File map

| File | Role |
|---|---|
| `src/maker.rs` | **Create.** Fair value, desired quotes, inventory gates, unit tests. |
| `src/structs.rs` | Add `MakerAction` + `TradeCommand::Maker`. |
| `src/main.rs` | Statics + CLI parse; `mod maker`. |
| `src/evaluate_arbitrage.rs` | Early exit into maker path when `MAKER_ENABLED`. |
| `src/trade.rs` | Maker reconcile loop: post/cancel, inventory from fills. |
| `src/arb_forensics.rs` | `MakerEvent` variant + serialize. |
| `README.md` | How to run, risks, interaction with `--trade` / arb. |

---

### Task 1: CLI flags and statics

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Produces:
  - `pub static MAKER_ENABLED: AtomicBool` (default false)
  - `pub static MAKER_NOTIONAL: AtomicI16` (default 10) — quote-currency dollars cap per quote and inventory
  - `pub static MAKER_OFFSET_BPS: AtomicI16` (default 5)
  - `pub const FEE_MAKER_BPS: i16 = 16` — documented assumption

- [ ] **Step 1: Add statics beside `MOMENTUM_ENABLED`**

```rust
pub static MAKER_ENABLED: AtomicBool = AtomicBool::new(false);
pub static MAKER_NOTIONAL: AtomicI16 = AtomicI16::new(10);
pub static MAKER_OFFSET_BPS: AtomicI16 = AtomicI16::new(5);
/// Assumed Kraken mid-tier maker fee (bps). Not fetched from TradeVolume in MVP.
pub const FEE_MAKER_BPS: i16 = 16;
```

- [ ] **Step 2: Parse in `Config::initialize`**

```rust
if args.contains(&"--maker".to_string()) {
    MAKER_ENABLED.store(true, Ordering::Relaxed);
}
if let Some(v) = parse_arg_value(&args, "--maker-notional") {
    MAKER_NOTIONAL.store(v.max(1), Ordering::Relaxed);
}
if let Some(v) = parse_arg_value(&args, "--maker-offset-bps") {
    MAKER_OFFSET_BPS.store(v.max(0), Ordering::Relaxed);
}
log::info!(
    "Maker: enabled={}, notional={}, offset_bps={}, fee_maker_bps={}",
    MAKER_ENABLED.load(Ordering::Relaxed),
    MAKER_NOTIONAL.load(Ordering::Relaxed),
    MAKER_OFFSET_BPS.load(Ordering::Relaxed),
    FEE_MAKER_BPS,
);
```

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat(maker): add --maker CLI flags and statics"
```

---

### Task 2: Maker quote math (TDD)

**Files:**
- Create: `src/maker.rs`
- Modify: `src/main.rs` (`mod maker;`)

**Interfaces:**
- Consumes: `PairData`, knobs, inventory coin position for the target pair.
- Produces:
  - `pub fn sibling_fair_mid(target, sibling, target_stable, sibling_stable) -> Option<f64>`
  - `pub fn desired_quotes(...) -> Option<MakerDesire>`
  - `pub struct MakerDesire { fair_mid, bid_price: Option<f64>, ask_price: Option<f64>, volume_coin, reason: &'static str }`

**Fair mid (same FX shape as arb/momentum):**
Convert sibling mid into target quote currency:

```text
sib_mid = (sibling.bid + sibling.ask) / 2
fx = target_stable.mid / sibling_stable.mid   // mid = (bid+ask)/2
fair_mid = sib_mid * fx
```

Require all prices > 0.

**Desired quotes:**
- `tick = 10^(-price_decimals)`
- `offset = fair_mid * offset_bps / 10_000`
- Ideal bid = `fair_mid - offset`; ideal ask = `fair_mid + offset`
- Improve inside spread when possible:
  - `bid = min(ideal_bid, ask - tick)` then `bid = max(bid, bid_book)` only if still `< ask` (improve: `bid_book + tick` if that is ≤ ideal and `< ask`)
  - Simpler MVP rule (implement this):
    - `bid = (target.bid + tick).min(ideal_bid).min(target.ask - tick)` if that `bid > 0` and `bid < target.ask`
    - `ask = (target.ask - tick).max(ideal_ask).max(target.bid + tick)` if that `ask > target.bid`
  - If computed bid ≥ ask or no room inside spread → that side `None`
- Volume: `volume = notional / fair_mid`, then clamp to `order_min` / reject if below mins
- Inventory (coin units, positive = long target base):
  - If `inventory * fair_mid >= notional` → suppress bid (`None`)
  - If `inventory * fair_mid <= -notional` → suppress ask
  - If `|inventory| * fair_mid >= notional` both sides saturated → only allow reducing side (already covered)
- Adverse lean: if ideal bid is below `target.bid` with no improve room, bid `None` (do not cross). Same for ask.

- [ ] **Step 1: Write failing tests** in `src/maker.rs` `#[cfg(test)]`:
  - `fair_mid_applies_fx`
  - `quotes_inside_spread_with_offset`
  - `suppresses_bid_when_inventory_long_capped`
  - `returns_none_when_spread_too_tight`
  - `volume_from_notional`

- [ ] **Step 2: Run tests — expect FAIL**

```bash
cargo test --lib maker::
```

- [ ] **Step 3: Implement `sibling_fair_mid` + `desired_quotes`**

- [ ] **Step 4: `cargo test --lib maker::` PASS; commit**

```bash
git add src/maker.rs src/main.rs
git commit -m "feat(maker): sibling fair-value quote math with inventory caps"
```

---

### Task 3: TradeCommand + forensics events

**Files:**
- Modify: `src/structs.rs`
- Modify: `src/arb_forensics.rs`

**Interfaces:**
- Produces:

```rust
pub enum TradeCommand {
    Arb(OrderInfo),
    Momentum(MomentumOrder),
    Maker(MakerAction),
}

pub struct MakerAction {
    pub pair_name: &'static str,
    pub sibling_name: &'static str,
    pub fair_mid: f64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub volume_coin: f64,
    pub price_decimals: usize,
    pub volume_decimals: usize,
    pub inventory_coin: f64,
    pub send_timestamp: u128,
    pub updated_pair_kraken_ts: f64,
    pub opportunity_id: u64,
    pub reason: &'static str,
}
```

```rust
// arb_forensics.rs
ForensicsEvent::Maker(MakerEvent)

pub struct MakerEvent {
    pub event: &'static str, // "maker_quote" | "maker_cancel" | "maker_fill" | "maker_inventory"
    pub opportunity_id: u64,
    pub pair: &'static str,
    pub sibling: &'static str,
    pub side: &'static str,       // "buy" | "sell" | "both" | "none"
    pub fair_mid: f64,
    pub price: f64,
    pub volume: f64,
    pub userref: i32,
    pub inventory_coin: f64,
    pub reason: &'static str,
    pub event_ts_ns: u128,
}
```

Wire `Maker` arm in `writer_loop` serialize match.

- [ ] Implement types + forensics; compile; commit

```bash
git commit -m "feat(maker): TradeCommand::Maker and forensics MakerEvent"
```

---

### Task 4: Evaluation hook (maker exclusive)

**Files:**
- Modify: `src/evaluate_arbitrage.rs`

**Interfaces:**
- Consumes: `MAKER_ENABLED`, `maker::desired_quotes`, inventory snapshot (atomic — see Task 5).
- Behavior: at top of `evaluate_arbitrage`, if `MAKER_ENABLED`:
  1. Require books/pairs ready (same guards as arb for X/Y + stables).
  2. Target = updated pair `x`; sibling = `y`.
  3. Load inventory for `x` from `MAKER_INVENTORY` map or per-pair atomics — MVP: single global `MAKER_INVENTORY_COIN: AtomicI64` scaled by 1e8 **only if quoting one pair**, OR pass `0.0` from eval and let trade thread enforce inventory (simpler).

**MVP inventory ownership:** Trade thread owns inventory. Eval sends desires assuming inventory from a process-global `AtomicI64` (`MAKER_INV_COIN_E8`) updated by trade thread on fills. Eval reads it for one-sided suppression.

```rust
if MAKER_ENABLED.load(Ordering::Relaxed) {
    maybe_send_maker(...);
    return;
}
```

`maybe_send_maker` builds `MakerAction` via `desired_quotes`, `try_log` a `maker_quote` decision event, `send_trade_command` **without** treating maker like arb for busy-drop sensitivity: still use try_send, but trade thread must clear busy quickly (Task 5). Prefer `try_send`; if full, log `channel_full` (latest quote lost — acceptable if cancel path is frequent).

- [ ] Implement early return + tests that maker path does not call dual_walk when enabled (unit test on `desired_quotes` integration optional; smoke via maker module tests).
- [ ] Commit

```bash
git commit -m "feat(maker): route BBO updates to maker when --maker set"
```

---

### Task 5: Trading-thread maker reconcile

**Files:**
- Modify: `src/trade.rs`
- Modify: `src/main.rs` (export `MAKER_INV_COIN_E8: AtomicI64`)

**Interfaces:**
- State in trade thread (not shared except inventory atomic):

```rust
struct RestingQuote {
    userref: i32,
    price: f64,
    volume: f64,
}
struct MakerPairState {
    bid: Option<RestingQuote>,
    ask: Option<RestingQuote>,
}
// HashMap<&'static str, MakerPairState> — MVP may keep only last pair
```

**On `TradeCommand::Maker(action)`:**
1. Do **not** apply the 10ms staleness skip as a hard block for cancels; always cancel if desired side is `None` while resting. For new posts, skip if `time_diff >= 0.010` (same gate as arb) to avoid posting on stale books.
2. Reconcile bid:
   - If resting and (desired None OR price changed by ≥ 1 tick OR volume changed): `cancelOrder` with `txid: [userref.to_string()]` (Kraken accepts userref), forensics `maker_cancel`.
   - If desired Some and (no resting or just cancelled): `addOrder` limit GTC buy, `"oflags": "post"`, forensics `maker_quote`.
3. Same for ask (sell).
4. Clear `TRADER_BUSY` immediately after WS sends (do not wait for fills).
5. `ownTrades` listener: when fill `userref` matches a resting quote, update `MAKER_INV_COIN_E8`, clear/reduce resting side, forensics `maker_fill` + `maker_inventory`.

**addOrder shape:**

```json
{
  "event": "addOrder",
  "token": "...",
  "type": "buy",
  "ordertype": "limit",
  "price": "...",
  "volume": "...",
  "pair": "XBT/USD",
  "userref": "12345",
  "oflags": "post"
}
```

No `timeinforce: IOC` — default GTC.

**cancelOrder shape:**

```json
{
  "event": "cancelOrder",
  "token": "...",
  "txid": ["12345"]
}
```

**Exit policy:** No market sell. If inventory non-zero, keep quoting only the reducing side until flat or capped.

- [ ] Implement reconcile + fill inventory updates
- [ ] Extend `run_trading_thread` match arm for `Maker`
- [ ] Commit

```bash
git commit -m "feat(maker): post-only quote reconcile and inventory tracking"
```

---

### Task 6: README + verify

**Files:**
- Modify: `README.md`

- [ ] Document:

```bash
# Dry-run desires to forensics JSONL (no orders)
./target/release/arbitrage --maker --maker-notional 10 --maker-offset-bps 5

# Live maker (posts require --trade)
./target/release/arbitrage --trade --maker --maker-notional 10 --maker-offset-bps 5 --colocated
```

Risks: inventory can strand up to ~notional; cancel latency is the edge; maker fee assumed 16bps; **when `--maker` is set, arb and momentum do not run**.

- [ ] `cargo test` full suite green
- [ ] Commit

```bash
git commit -m "docs: document maker mode usage and risks"
```

---

### Task 7: Push

```bash
git push -u origin HEAD
```

---

## Self-review (spec coverage)

| Requirement | Task |
|---|---|
| Sibling BBO FX fair value | Task 2 |
| Resting LIMIT inside spread | Tasks 2, 5 |
| Cancel/replace on adverse move | Tasks 2, 5 |
| No market-sell primary exit | Task 5 |
| Hard notional/inventory caps | Tasks 1, 2, 5 |
| Forensics JSONL | Task 3, 5 |
| Off by default; arb interaction | Tasks 1, 4, 6 |
| ≤3 knobs | Task 1 |
| TDD + cargo test | Tasks 2, 6 |
| README | Task 6 |

No placeholders remaining. FEE_MAKER constant documented; TradeVolume maker fetch deferred.
