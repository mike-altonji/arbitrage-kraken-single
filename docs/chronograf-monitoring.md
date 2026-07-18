# Chronograf / InfluxQL monitoring reference

This project uses **InfluxDB 1.x + Chronograf** (not Grafana). Use this doc to rebuild or extend Chronograf dashboards.

Replace `"RP_NAME"` with the value of `RP_NAME` from `.env` (also used by continuous queries in [`scripts/setup_influxdb.sh`](../scripts/setup_influxdb.sh)).

Notes:

- Most latency raw points are written into `RP_NAME`.
- `trade_message_receive_speed`, `arbitrage_opportunity`, and `momentum_execution` are written **without** an explicit retention policy, so they usually land on the database **default** RP. If a query returns empty, try with and without `"RP_NAME".` prefix, or check Chronograf’s database/RP dropdown.
- Depth fields (`depth_multiplier`, `blended_roi`, etc.) only exist on opportunities logged after that code was deployed.

---

## Existing dashboard panels

These match the Chronograf cells already in use.

### 1. Ingestion Latency by Pair (table)

**Visualizes:** 5-minute CQ aggregates of Kraken→ingest latency per pair (`max` / `mean` / `min` / percentiles).

```sql
SELECT
  max_latency,
  mean_latency,
  min_latency,
  p01_latency,
  p50_latency,
  p99_latency
FROM "RP_NAME"."kraken_ingestion_latency_aggregates"
WHERE time > now() - 24h
```

Optional — one row group per pair (if Chronograf shows the `pair` tag):

```sql
SELECT
  max("max_latency") AS max_latency,
  mean("mean_latency") AS mean_latency,
  min("min_latency") AS min_latency
FROM "RP_NAME"."kraken_ingestion_latency_aggregates"
WHERE time > now() - 24h
GROUP BY "pair"
```

**Source:** continuous query `cq_kraken_ingestion_latency_aggregates` over raw `kraken_ingestion_latency`.

### 2. Spread / ingestion latency for one pair (line)

**Visualizes:** Max ingest latency over time for a single pair (example: `SRM/USD`).

```sql
SELECT max_latency, mean_latency
FROM "RP_NAME"."kraken_ingestion_latency_aggregates"
WHERE time > now() - 24h AND "pair" = 'SRM/USD'
```

Raw (non-aggregated) alternative:

```sql
SELECT mean("latency")
FROM "RP_NAME"."kraken_ingestion_latency"
WHERE time > now() - 6h AND "pair" = 'SRM/USD'
GROUP BY time(10s) fill(null)
```

### 3. Arbitrage Details (table)

**Visualizes:** Individual opportunity rows (prices, volumes, notional).

```sql
SELECT
  "limiting_volume",
  "pair1_amount_in",
  "pair1_ask",
  "pair1_ask_volume",
  "pair1_bid",
  "pair1_bid_volume",
  "pair2_ask",
  "pair2_bid",
  "roi"
FROM "arbitrage_opportunity"
WHERE time > now() - 24h
```

Tags `pair1` / `pair2` appear as columns depending on Chronograf settings. Prefer the Explore field picker if the above omits tags.

**Meaning note (depth walk):** `limiting_volume` is now the **depth-walk volume**, not only `min(L1 ask, L1 bid)`.

### 4. Trade Message Receive Latency (line)

**Visualizes:** Time from `OrderInfo` creation on the listener to receipt on the trading thread (seconds).

```sql
SELECT "duration"
FROM "trade_message_receive_speed"
WHERE time > now() - 24h
```

Or smoothed:

```sql
SELECT mean("duration")
FROM "trade_message_receive_speed"
WHERE time > now() - 24h
GROUP BY time(10s) fill(null)
```

### 5. Listener Loop Speed Aggregates (line)

**Visualizes:** p50 (and optionally other percentiles) of per-message listener loop duration from the 5m CQ.

```sql
SELECT p50_duration, p90_duration, p99_duration
FROM "RP_NAME"."listener_loop_speed_aggregates"
WHERE time > now() - 24h
```

Related — arbitrage evaluation speed CQ:

```sql
SELECT p50_duration, p90_duration, p99_duration
FROM "RP_NAME"."arbitrage_evaluation_speed_aggregates"
WHERE time > now() - 24h
```

---

## New depth-aware panels (add these)

Create new Chronograf cells with the queries below after deploying code that writes the new fields.

### Depth multiplier over time

**Visualizes:** How much extra size the book walk unlocks vs old L1 `min(ask,bid)` sizing. Values ≫ 1 ⇒ walk matters.

```sql
SELECT mean("depth_multiplier")
FROM "arbitrage_opportunity"
WHERE time > now() - 6h
GROUP BY time(1m), "trigger" fill(null)
```

### Tip ROI vs blended ROI

**Visualizes:** BBO tip edge (`roi`) vs depth-weighted edge (`blended_roi`).

```sql
SELECT mean("roi") AS tip_roi, mean("blended_roi") AS blended_roi
FROM "arbitrage_opportunity"
WHERE time > now() - 6h
GROUP BY time(1m) fill(null)
```

### ROI gap (tip − blended)

**Visualizes:** How much tip ROI overstates the depth-filled opportunity.

```sql
SELECT mean("roi_gap")
FROM "arbitrage_opportunity"
WHERE time > now() - 6h
GROUP BY time(1m), "trigger" fill(null)
```

### Depth volume vs L1 limiting volume

**Visualizes:** Walk size vs classic top-of-book cap.

```sql
SELECT mean("depth_volume") AS depth_volume, mean("l1_limiting_volume") AS l1_limiting_volume
FROM "arbitrage_opportunity"
WHERE time > now() - 6h
GROUP BY time(1m) fill(null)
```

### Expected PnL (modeled quote PnL)

**Visualizes:** Expected proceeds − cost after spot fees used in the walk (not realized fill PnL).

```sql
SELECT mean("expected_pnl")
FROM "arbitrage_opportunity"
WHERE time > now() - 6h
GROUP BY time(1m), "trigger" fill(null)
```

### Balance-capped rate

**Visualizes:** Fraction of opportunities limited by wallet balance (0–1). Uses float field `balance_limited_f` so `mean()` works.

```sql
SELECT mean("balance_limited_f")
FROM "arbitrage_opportunity"
WHERE time > now() - 6h
GROUP BY time(1m) fill(null)
```

### Opportunity count by trigger

**Visualizes:** Buy-side triggers (`ask_improved`) vs sell-side triggers (`bid_improved`).

```sql
SELECT count("depth_volume")
FROM "arbitrage_opportunity"
WHERE time > now() - 6h
GROUP BY time(1m), "trigger" fill(null)
```

### Extended Arbitrage Details table

Add these fields alongside the existing table columns:

```sql
SELECT
  "limiting_volume",
  "depth_volume",
  "l1_limiting_volume",
  "depth_multiplier",
  "roi",
  "blended_roi",
  "roi_gap",
  "expected_pnl",
  "pair1_amount_in",
  "vwap_ask",
  "vwap_bid",
  "limit_buy_price"
FROM "arbitrage_opportunity"
WHERE time > now() - 24h
```

Also show tag **`trigger`** if Chronograf exposes it for the measurement.

---

## Field cheat sheet (`arbitrage_opportunity`)

| Field / tag | Meaning |
|-------------|---------|
| `limiting_volume` / `depth_volume` | Size after depth walk |
| `l1_limiting_volume` | `min(pair1_ask_volume, pair2_bid_volume)` baseline |
| `depth_multiplier` | `depth_volume / l1_limiting_volume` |
| `roi` | BBO tip ROI |
| `blended_roi` | Depth-weighted ROI |
| `roi_gap` | `roi - blended_roi` |
| `expected_pnl` | Modeled PnL in pair1 quote (pair2 proceeds converted via stables) |
| `pair1_amount_in` | Expected spend on buy leg |
| `trigger` (tag) | `ask_improved` (updated pair is buy leg) or `bid_improved` (updated pair is sell leg) |
| `balance_limited_f` | 1.0 if balance-capped, else 0.0 |
| `volume_limited_by_balance` | Boolean twin (less useful for `mean()`) |

---

## Momentum panels (`momentum_execution`, only with `--momentum`)

Each point is one same-pair round trip: IOC buy, timed hold, market sell.
Tags: `pair`, `trigger_pair`, `outcome`. The hold time is sampled log-uniformly
in [1, 1000] ms per trade — regress `realized_pnl` against `hold_ms`.

### Realized PnL vs hold time (scatter via table export)

```sql
SELECT "hold_ms", "realized_pnl", "gap_bps"
FROM "momentum_execution"
WHERE time > now() - 24h
```

### Momentum PnL over time

```sql
SELECT sum("realized_pnl")
FROM "momentum_execution"
WHERE time > now() - 6h
GROUP BY time(5m), "pair" fill(null)
```

### Outcome mix

```sql
SELECT count("realized_pnl")
FROM "momentum_execution"
WHERE time > now() - 24h
GROUP BY time(15m), "outcome" fill(null)
```

---

## JSONL forensics funnel (not in Chronograf)

Planned slices, drop decisions (`sent`, `trader_busy`, …), and actual fill outcomes live in:

```text
logs/arb_events_{startup_timestamp}.jsonl
```

Summarize with:

```bash
python3 scripts/analyze_arb_events.py logs/arb_events_*.jsonl
python3 scripts/analyze_arb_events.py logs/arb_events_*.jsonl --since 2026-07-18T20:00:00Z
python3 scripts/analyze_arb_events.py logs/arb_events_*.jsonl --since 1721332800 --until 1721419200
```

`--since` / `--until` accept ISO-8601 or unix seconds. Opportunities are filtered on
`eval_ts_ns`; executions / momentum on `event_ts_ns` (events from older builds without
those fields are dropped when a window is set).

That prints:

1. **Decision funnel** — `sent` / `trader_busy` / `channel_full` / `below_min_*` / `no_depth_volume`
2. **Walk `stop_reason`** — why the depth walk stopped
3. **`trigger` mix**
4. **Expected PnL** distribution and top pairs
5. **Executions** (only with `--trade`) — outcomes, expected vs realized PnL joined on `opportunity_id`, slippage bps
6. **Win rate vs `trigger`** — among `filled` + `partial_buy` only; win = `realized_pnl > 0`
7. **Slippage + PnL residual vs `data_age_ms`** — mean residual and RMSE of `realized − expected`, plus mean buy/sell slippage
8. **PnL residual vs `blended_roi`** and vs **`depth_multiplier`**
9. **Momentum win rate vs `hold_ms`** — same win definition

New forensics fields (post this change): opportunities carry `l1_limiting_volume` /
`depth_multiplier`; executions carry `event_ts_ns`, `data_age_ms`, `channel_delay_ms`.

Chronograf is for time-series aggregates; use the script when you want “what did we plan vs what did we get?”

---

## How to add a cell in Chronograf

1. Open your dashboard → **Add Cell**
2. Select database (and RP if prompted)
3. Paste an InfluxQL query above (or use the query builder, then switch to raw)
4. Choose visualization: **line** for time series, **table** for details
5. Name the cell to match the titles in this doc
6. Save the dashboard
