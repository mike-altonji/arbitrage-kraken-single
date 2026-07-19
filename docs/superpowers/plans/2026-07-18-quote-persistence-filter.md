# Quote Persistence Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Suppress phantom thin-pair BBO improvements by requiring an improved bid (or ask) to survive N subsequent BBO-changing book updates before an arb trade may be sent.

**Architecture:** Add a per-listener `QuotePersistTracker` that ages pending improved quotes on every BBO change for that pair. `--persist-updates N` (default 0 = off) is the single knob. On first sighting of a tradeable improvement the evaluator still runs dual-walk and logs `decision=awaiting_persist` but does not send; after N confirming updates at a still-good price it allows the normal send path; if the quote worsens first it logs `decision=persist_failed`. Update-count persistence is preferred over `--persist-ms` because Kraken books arrive as discrete WS updates and the forensic failure mode is “L0 vanished on the next update,” which maps directly to confirmation counts. Counting only BBO-changing updates (the path that already calls `evaluate_arbitrage`) is slightly more conservative than counting deep-only touches and needs no listener hot-path restructuring.

**Tech Stack:** Rust (existing tokio listener / atomic CLI knobs / arb_forensics JSONL). Verify with `cargo test`.

## Global Constraints

- One knob: `--persist-updates N`, default `0` (current behavior / off).
- Decision strings for forensics: `awaiting_persist`, `persist_failed` (plus existing `sent`, `trader_busy`, …).
- Must not break dual-walk arb, ROI buffer, depth haircut, colocated, trade mode, or momentum priority.
- Follow `src/main.rs` CLI pattern: `parse_arg_value`, `AtomicI16` static, log risk knobs at startup.
- Hot path must never block; forensics via existing `try_log` only.
- TDD where practical; `cargo test` must pass before push.

## File Structure

| File | Role |
|------|------|
| `src/quote_persist.rs` (new) | Pure tracker: pending bid/ask per pair idx, observe API, unit tests |
| `src/main.rs` | `PERSIST_UPDATES` static + CLI parse + startup log |
| `src/evaluate_arbitrage.rs` | Wire tracker into direction selection + send gate + fail logging |
| `src/listener.rs` | Own a `QuotePersistTracker`, pass `&mut` into `evaluate_arbitrage` |
| `src/threads.rs` | No API change (tracker created inside listener) |
| `README.md` | Document `--persist-updates N` |
| `scripts/analyze_arb_events.py` | Optional: no code change required (Counter already prints new decisions) |

---

### Task 1: QuotePersistTracker module (TDD)

**Files:**
- Create: `src/quote_persist.rs`
- Modify: `src/main.rs` (add `mod quote_persist;`)

**Interfaces:**
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub enum PersistVerdict {
      /// N==0, or no pending and this tick did not improve → caller uses raw BBO flags.
      Inactive,
      /// Improvement armed (or still waiting); do not send; log `awaiting_persist` if opportunity exists.
      Awaiting,
      /// Survived N BBO updates at price still >= (bid) / <= (ask) pending; may send.
      Ready,
      /// Pending quote worsened/vanished before N confirms; log `persist_failed`, do not send.
      Failed,
  }

  pub struct QuotePersistTracker { /* per-idx bid/ask pending */ }

  impl QuotePersistTracker {
      pub fn new(num_pairs: usize, required_updates: u16) -> Self;
      /// Observe one BBO-changing update for `idx`. Call once per evaluate entry.
      pub fn observe_bid(&mut self, idx: usize, bid_price: f64, improved: bool) -> PersistVerdict;
      pub fn observe_ask(&mut self, idx: usize, ask_price: f64, improved: bool) -> PersistVerdict;
  }
  ```
- Bid still-good: `bid_price + 1e-12 >= pending_price`. Ask still-good: `ask_price > 0.0 && ask_price <= pending_price + 1e-12`.
- On `improved` while Inactive: arm pending at current price with `remaining = required_updates`; return `Awaiting`.
- On each later observe while pending and still-good: decrement `remaining`; at 0 clear pending and return `Ready`.
- On pending and not still-good: clear pending; if this tick also `improved`, re-arm at new price and return `Awaiting`; else return `Failed`.
- `required_updates == 0`: always `Inactive` (no state).
- Further improvement while awaiting (better price): refresh pending price and reset `remaining` to required (restart clock on the new level).

- [ ] **Step 1: Write the failing tests** in `src/quote_persist.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_required_always_inactive() {
        let mut t = QuotePersistTracker::new(4, 0);
        assert_eq!(t.observe_bid(2, 100.0, true), PersistVerdict::Inactive);
        assert_eq!(t.observe_bid(2, 100.0, false), PersistVerdict::Inactive);
    }

    #[test]
    fn one_update_awaits_then_ready() {
        let mut t = QuotePersistTracker::new(4, 1);
        assert_eq!(t.observe_bid(2, 100.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 100.0, false), PersistVerdict::Ready);
        // cleared; no pending
        assert_eq!(t.observe_bid(2, 100.0, false), PersistVerdict::Inactive);
    }

    #[test]
    fn phantom_bid_fails_before_confirm() {
        let mut t = QuotePersistTracker::new(4, 1);
        assert_eq!(t.observe_bid(2, 100.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 99.0, false), PersistVerdict::Failed);
    }

    #[test]
    fn ask_side_symmetric() {
        let mut t = QuotePersistTracker::new(4, 1);
        assert_eq!(t.observe_ask(3, 50.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_ask(3, 50.0, false), PersistVerdict::Ready);
        assert_eq!(t.observe_ask(3, 51.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_ask(3, 52.0, false), PersistVerdict::Failed);
    }

    #[test]
    fn two_updates_need_two_confirms() {
        let mut t = QuotePersistTracker::new(4, 2);
        assert_eq!(t.observe_bid(2, 10.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.0, false), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.1, false), PersistVerdict::Ready); // still >= 10
    }

    #[test]
    fn better_price_while_awaiting_resets_counter() {
        let mut t = QuotePersistTracker::new(4, 2);
        assert_eq!(t.observe_bid(2, 10.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.5, true), PersistVerdict::Awaiting); // reset
        assert_eq!(t.observe_bid(2, 10.5, false), PersistVerdict::Awaiting); // 1 of 2
        assert_eq!(t.observe_bid(2, 10.5, false), PersistVerdict::Ready);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib quote_persist -- --nocapture`
Expected: compile fail (module missing) or test fail.

- [ ] **Step 3: Implement `QuotePersistTracker`**

```rust
//! Quote persistence filter: require an improved BBO to survive N later BBO updates.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistVerdict { Inactive, Awaiting, Ready, Failed }

#[derive(Clone, Copy, Debug)]
struct Pending { price: f64, remaining: u16 }

pub struct QuotePersistTracker {
    required: u16,
    bids: Vec<Option<Pending>>,
    asks: Vec<Option<Pending>>,
}

impl QuotePersistTracker {
    pub fn new(num_pairs: usize, required_updates: u16) -> Self {
        Self {
            required: required_updates,
            bids: vec![None; num_pairs],
            asks: vec![None; num_pairs],
        }
    }

    pub fn observe_bid(&mut self, idx: usize, bid_price: f64, improved: bool) -> PersistVerdict {
        Self::observe_side(&mut self.bids, self.required, idx, bid_price, improved, true)
    }

    pub fn observe_ask(&mut self, idx: usize, ask_price: f64, improved: bool) -> PersistVerdict {
        Self::observe_side(&mut self.asks, self.required, idx, ask_price, improved, false)
    }

    fn still_good(is_bid: bool, current: f64, pending_price: f64) -> bool {
        if is_bid {
            current + 1e-12 >= pending_price
        } else {
            current > 0.0 && current <= pending_price + 1e-12
        }
    }

    fn observe_side(
        slots: &mut [Option<Pending>],
        required: u16,
        idx: usize,
        price: f64,
        improved: bool,
        is_bid: bool,
    ) -> PersistVerdict {
        if required == 0 || idx >= slots.len() {
            return PersistVerdict::Inactive;
        }
        let mut failed = false;
        if let Some(p) = slots[idx] {
            if Self::still_good(is_bid, price, p.price) {
                let better = if is_bid { price > p.price + 1e-12 } else { price + 1e-12 < p.price };
                if improved && better {
                    slots[idx] = Some(Pending { price, remaining: required });
                    return PersistVerdict::Awaiting;
                }
                let left = p.remaining.saturating_sub(1);
                if left == 0 {
                    slots[idx] = None;
                    return PersistVerdict::Ready;
                }
                slots[idx] = Some(Pending { price: p.price, remaining: left });
                return PersistVerdict::Awaiting;
            } else {
                slots[idx] = None;
                failed = true;
            }
        }
        if improved {
            slots[idx] = Some(Pending { price, remaining: required });
            return PersistVerdict::Awaiting;
        }
        if failed { PersistVerdict::Failed } else { PersistVerdict::Inactive }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib quote_persist`
Expected: all `quote_persist` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/quote_persist.rs src/main.rs
git commit -m "$(cat <<'EOF'
feat: add QuotePersistTracker for BBO survival gating

EOF
)"
```

---

### Task 2: CLI knob `--persist-updates`

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Produces: `pub static PERSIST_UPDATES: AtomicI16 = AtomicI16::new(0);`
- Parse `--persist-updates N` via existing `parse_arg_value`; clamp store to `>= 0` (i16 already); log in the existing risk-knobs `log::info!`.

- [ ] **Step 1: Add static + parse + log**

Beside the other risk knobs:

```rust
pub static PERSIST_UPDATES: AtomicI16 = AtomicI16::new(0);

// in Config::initialize:
if let Some(v) = parse_arg_value(&args, "--persist-updates") {
    PERSIST_UPDATES.store(v.max(0), std::sync::atomic::Ordering::Relaxed);
}
log::info!(
    "Risk knobs: max_walk_depth={}, depth_haircut_pct={}, roi_buffer_bps={}, momentum={}, persist_updates={}",
    ...
    PERSIST_UPDATES.load(...),
);
```

- [ ] **Step 2: Commit**

```bash
git add src/main.rs
git commit -m "$(cat <<'EOF'
feat: add --persist-updates CLI risk knob

EOF
)"
```

---

### Task 3: Wire tracker into evaluate + listener (TDD on gate behavior)

**Files:**
- Modify: `src/evaluate_arbitrage.rs`
- Modify: `src/listener.rs`

**Interfaces:**
- Consumes: `QuotePersistTracker`, `PERSIST_UPDATES`, `PersistVerdict`.
- Changes `evaluate_arbitrage` signature to take `persist: &mut QuotePersistTracker`.
- Listener constructs tracker once after books are sized:
  ```rust
  let required = crate::PERSIST_UPDATES.load(Ordering::Relaxed).max(0) as u16;
  let mut persist = QuotePersistTracker::new(pair_data_vec.len(), required);
  ```
  Pass `&mut persist` into every `evaluate_arbitrage` call.
- Direction selection:
  ```rust
  let bid_v = persist.observe_bid(idx, x_pair.bid_price, bbo_change.bid_improved);
  let ask_v = persist.observe_ask(idx, x_pair.ask_price, bbo_change.ask_improved);

  let consider_bid = matches!(bid_v, PersistVerdict::Awaiting | PersistVerdict::Ready | PersistVerdict::Failed)
      || (bid_v == PersistVerdict::Inactive && bbo_change.bid_improved);
  // same for ask
  if !consider_bid && !consider_ask { return; }
  ```
- When processing bid-improved direction:
  - `Failed` → call a small `log_persist_failed(...)` (ArbOpportunityEvent with `decision: "persist_failed"`, zero depth, trigger `bid_improved`) and skip send/momentum/dual_walk for that side.
  - `Awaiting` → run momentum (unchanged priority) then `process_arbitrage_opportunity` with `persist_hold: Some("awaiting_persist")` so send is skipped but full opportunity is logged.
  - `Ready` or (`Inactive` && improved) → existing path (`persist_hold: None`).
- Extend `process_arbitrage_opportunity` with `persist_hold: Option<&'static str>`:
  ```rust
  let decision = if let Some(hold) = persist_hold {
      hold // "awaiting_persist"
  } else if let Some(reason) = guardrail_failure_reason(...) {
      reason
  } else {
      send_trade_command(...)
  };
  ```
- Momentum: only on real `bid_improved` when verdict is `Awaiting` or allow-send path; do **not** fire momentum on `Ready` ticks that are confirmations without a fresh improvement (avoid double-firing). Spec: `if bbo_change.bid_improved && matches!(bid_v, Awaiting | Ready | Inactive) && MOMENTUM_ENABLED` — simpler rule: keep momentum gated on `bbo_change.bid_improved` only (unchanged), and when momentum consumes, skip arb as today even if Awaiting.

- [ ] **Step 1: Add unit test for decision override helper** (optional thin wrapper) or test `observe` integration via existing tracker tests (Task 1). Add one evaluate-level test if a pure helper is extracted:

```rust
fn persist_hold_label(v: PersistVerdict) -> Option<&'static str> {
    match v {
        PersistVerdict::Awaiting => Some("awaiting_persist"),
        PersistVerdict::Failed => Some("persist_failed"),
        _ => None,
    }
}
```

- [ ] **Step 2: Implement wiring** as described; update all `evaluate_arbitrage(` call sites (listener only).

- [ ] **Step 3: `cargo test`**

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/evaluate_arbitrage.rs src/listener.rs
git commit -m "$(cat <<'EOF'
feat: gate arb sends on quote persistence verdicts

EOF
)"
```

---

### Task 4: README + final verification

**Files:**
- Modify: `README.md` (Command-Line Arguments section)

- [ ] **Step 1: Document the flag**

After `--momentum`:

```markdown
- `--persist-updates N`: Require an improved bid/ask to survive N further BBO updates before sending an arb trade (default 0 = off). Logs `awaiting_persist` / `persist_failed` for forensics.
```

- [ ] **Step 2: Full test + clippy sanity**

Run:
```bash
cargo test
cargo clippy --all-targets -- -D warnings
```
Expected: PASS (fix clippy noise pre-existing — do not expand scope; at least `cargo test` must be green).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: document --persist-updates CLI flag

EOF
)"
```

---

## Self-Review

1. **Spec coverage:** Single knob ✓; update-count model + rationale ✓; awaiting/failed decisions ✓; dual-walk/ROI/haircut/colocated/trade/momentum untouched except send gate ✓; CLI Atomic pattern ✓; TDD ✓; README ✓; analyze script optional (Counter already generic) ✓.
2. **Placeholders:** None — code and commands are concrete.
3. **Type consistency:** `PersistVerdict`, `QuotePersistTracker::{new,observe_bid,observe_ask}`, `persist_hold: Option<&'static str>` used consistently across tasks.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-18-quote-persistence-filter.md`.

**Inline execution** (this session): execute tasks in order with TDD commits, then push `feat/quote-persistence-filter`.
