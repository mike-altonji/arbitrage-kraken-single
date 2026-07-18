# Dual Walk + Momentum — Design Spec

Date: 2026-07-18
Status: Approved (chat review, one amendment: momentum takes priority over arbitrage)

## Goal

Replace the fixed-leg walk-mode arbitrage evaluator with a provably optimal dual-pointer
depth walk, select a single trade direction per BBO event from the direction of the
improvement, expose explicit depth-risk knobs, and add an experimental momentum trade
mode (`--momentum`) that round-trips a single pair when its sibling pair jumps.

## Background

The current evaluator (`src/evaluate_arbitrage.rs`) evaluates both quote directions on
every BBO change and, per direction, fixes one leg at top-of-book while walking the
other book (`fixed_ask_walk_bids` / `fixed_bid_walk_asks`, selected by
`resolve_walk_mode`). Because marginal ROI is monotonically non-increasing as both
books are walked deeper (asks rise, bids fall), a greedy two-pointer walk over both
books at once is exactly optimal for expected PnL and costs O(n + m) with
`BOOK_DEPTH = 10` per side. The fixed-leg design is the special case where one pointer
never advances; it under-plans volume and relies on own-fill echoes to capture
multi-level opportunities iteratively.

Leg 1 executes as a limit IOC. Kraken fills limit IOC orders at book prices, so a
multi-level plan still executes as one order: limit price = deepest planned ask,
volume = total planned volume. Partial-fill handling (sell only what filled) already
exists and is unchanged.

## 1. Trigger detection (`src/orderbook.rs`)

`BboChange` gains two semantic flags computed in `sync_bbo` (which sees old and new
top-of-book values):

- `bid_improved`: bid price increased, OR bid volume increased at unchanged price.
- `ask_improved`: ask price decreased, OR ask volume increased at unchanged price.

Worsening moves (bid down, ask up, volume decreases) never set these flags. The
now-unused `ask_changed()` / `bid_changed()` helpers are removed.

## 2. Direction selection (`src/evaluate_arbitrage.rs`)

`resolve_walk_mode`, the `WalkMode` enum, and both fixed-leg walk functions are
deleted. For a BBO event on crypto pair X (sibling pair Y, stable pairs at idx 0/1):

- `bid_improved` on X → evaluate direction **buy Y, sell X**. Telemetry trigger tag:
  `bid_improved`.
- `ask_improved` on X → evaluate direction **buy X, sell Y**. Telemetry trigger tag:
  `ask_improved`.
- Both flags in one message (e.g. spread tightens from both sides) → evaluate both
  directions.
- Neither → no evaluation.

A `Trigger` enum (`BidImproved` / `AskImproved`) replaces `WalkMode` in plan/telemetry
plumbing.

### Momentum priority (amendment)

When `--momentum` is enabled and a `bid_improved` event fires, the momentum entry gate
is evaluated FIRST. If it enters (order sent), the arb evaluation for that event is
skipped. If the gate declines, arb evaluation proceeds as normal. Rationale: the flag
exists for experimentation; when toggled on, momentum behavior should be observable.

## 3. Dual-walk planner (`src/evaluate_arbitrage.rs`)

One function replaces both fixed-leg walks:

```text
dual_walk(book_buy, book_sell, pair1_stable, pair2_stable, balance, fee_spot, arb_fee,
          max_depth, haircut, roi_buffer) -> Option<DepthFill>

i = 0 (buy-book ask level), j = 0 (sell-book bid level)
ask_rem = haircut_volume(asks[0], level 0), bid_rem = haircut_volume(bids[0], level 0)

loop while i < min(ask_count, max_depth) and j < min(bid_count, max_depth):
    roi = roi_at_prices(asks[i].price, bids[j].price)        # includes arb_fee
    if roi <= 1 + roi_buffer: stop (unprofitable_level)
    trade = min(ask_rem, bid_rem, balance_remaining / (asks[i].price * (1 + fee_spot)))
    if balance-constrained: stop after this slice (balance_capped)
    record walk_ask and walk_bid slices for `trade`
    advance whichever level(s) exhausted; refill *_rem with haircut volume
```

- `haircut_volume(level, idx)`: level 0 volume is taken in full; levels ≥ 1 are
  multiplied by `DEPTH_HAIRCUT_PCT / 100`.
- Stop reasons: `unprofitable_level`, `balance_capped`, `book_depth_exhausted`
  (either book ran out of real levels), `depth_cap_reached` (hit `max_depth`).
- Outputs (`DepthFill`): total volume, `vwap_ask` (walked buy cost / volume),
  `vwap_bid` (walked sell proceeds / volume), `limit_buy_price` = deepest ask level
  touched, blended ROI at the two VWAPs, expected cost/proceeds/PnL in pair1 quote via
  the stable legs (same math as today), slices for both legs, `trigger` instead of
  `walk_mode`.
- Slice capacity doubles (`2 * BOOK_DEPTH` walked slices max, both legs).

The top-of-book ROI pre-filter in `evaluate_arbitrage` remains (cheap early exit) and
uses the same `1 + roi_buffer` threshold.

## 4. Risk knobs and flags (`src/main.rs`)

Statics following the existing `FEE_SPOT` atomic pattern, set once from CLI args at
startup:

| Static | CLI | Default | Meaning |
|---|---|---|---|
| `MAX_WALK_DEPTH: AtomicI16` | `--max-walk-depth N` | 10 | Max levels consumed per side |
| `DEPTH_HAIRCUT_PCT: AtomicI16` | `--depth-haircut N` | 70 | % of displayed volume planned at levels beyond L0 |
| `ROI_BUFFER_BPS: AtomicI16` | `--roi-buffer-bps N` | 2 | Marginal ROI must exceed 1 + buffer |
| `MOMENTUM_ENABLED: AtomicBool` | `--momentum` | false | Enable momentum trade mode |

## 5. Momentum trade (`src/momentum.rs`, new)

Directional round trip on one pair Y, triggered by a `bid_improved` event on sibling
pair X: buy Y at its current ask (limit IOC), hold briefly, market sell on Y — betting
Y follows X upward.

### Entry gate

```text
fx      = pair_Y_stable.bid / pair_X_stable.ask     # converts X quote -> Y quote
p_other = X.bid * fx
gap     = p_other - Y.ask                            # require gap > 0
exit_est = Y.ask + CONVERGENCE_FRACTION * gap        # CONVERGENCE_FRACTION = 0.5

enter iff exit_est * (1 - fee_spot) >= Y.ask * (1 + fee_spot)
```

If Y converges halfway to X's converted price, selling there covers both spot fees.
`exit_est` is optimistic by roughly Y's spread (actual exit crosses to a future bid).

### Sizing and execution

- Size: `min(Y.top_ask_volume, balance / (Y.ask * (1 + fee_spot)))`, subject to the
  existing min-order/min-cost guardrails.
- Hold time: sampled log-uniformly per trade, `hold_ms = 10^uniform(0, 3)` ∈ [1, 1000]
  ms, carried on the order and logged for experimentation (regress PnL vs hold time).
- Execution (in `trade.rs`): limit IOC buy on Y at `Y.ask`; collect fills via the
  existing ownTrades machinery; `sleep(hold_ms)`; market sell the filled volume on the
  same pair Y. Same staleness gate as arb orders. Realized PnL is same-currency (no FX
  conversion).

## 6. Trade channel

The trade channel payload changes from `OrderInfo` to:

```rust
enum TradeCommand {
    Arb(OrderInfo),
    Momentum(MomentumOrder),
}
```

`MomentumOrder`: pair name, trigger pair name, volume, limit price, decimals, hold_ms,
gap_bps, opportunity_id, send/kraken timestamps. Type change ripples through
`main.rs`, `threads.rs`, `listener.rs`, `evaluate_arbitrage.rs`, `trade.rs`.

## 7. Telemetry

- `walk_mode` renames to `trigger` (values `bid_improved` / `ask_improved`) in
  `ArbOpportunityEvent` (forensics) and the `arbitrage_opportunity` Influx measurement.
- New forensics event + Influx measurement `momentum_execution`: trigger pair, traded
  pair, gap_bps, hold_time_ms, requested/filled volumes, entry/exit VWAPs, fees,
  realized PnL, outcome.
- Docs updated: `README.md`, `docs/chronograf-monitoring.md` (InfluxQL `GROUP BY`
  clauses), `scripts/analyze_arb_events.py`.

## 8. Testing

- Dual walk: multi-level crossing on both sides simultaneously; haircut applied beyond
  L0; depth cap; ROI buffer respected; balance cap; each stop reason reachable;
  `limit_buy_price` = deepest ask; VWAPs and expected PnL correct via stable legs.
- `sync_bbo`: `bid_improved` / `ask_improved` for price moves and volume-at-same-price
  moves; worsening moves set neither.
- Momentum gate: enters when half-gap covers fees, declines at boundary and for
  negative gap; sizing respects balance; hold-time sampler stays within [1, 1000] ms.
- Existing walk-mode tests removed/replaced. `cargo test` and `cargo clippy` clean.

## Non-goals

- FX (stablecoin pair) moves as triggers — explicitly out of scope per user.
- Momentum exit on price target — fixed hold time only, for experimentation.
- Multi-order execution per leg — single limit IOC + single market order, as today.
