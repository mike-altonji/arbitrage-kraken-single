//! Maker mode (`--maker`): resting post-only quotes sized from sibling-pair FX fair value.
//!
//! When enabled, arb and momentum are disabled. Quotes lean inside the target
//! pair's spread around the sibling-converted fair mid, with per-pair notional
//! and inventory caps. The evaluator (listener threads) publishes the latest
//! *desired* quotes per pair into a shared registry; the trade thread owns the
//! *live* order state and converges live → desired (cancel/replace).
//!
//! Registry semantics: latest-wins per pair. A desire with both sides `None`
//! means "cancel everything on this pair" — it is published whenever the data
//! needed to price safely is unavailable (pair offline, book not ready, no
//! fair value), so bad data pulls quotes instead of leaving them resting.

use crate::arb_forensics::next_opportunity_id;
use crate::orderbook::OrderBook;
use crate::structs::PairData;
use crate::{FEE_MAKER, MAKER_MIN_EDGE_BPS, MAKER_NOTIONAL, MAKER_OFFSET_BPS};
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

/// Scale factor for inventory storage (coin units × 1e8, stored as i64 to
/// avoid float accumulation drift).
pub const INV_SCALE: f64 = 100_000_000.0;

/// Relative volume difference below which a resting quote is left alone.
/// Cancel/replace for sub-20% size drift would burn queue position (and
/// Kraken rate-limit points) for no meaningful edge.
pub const VOLUME_DRIFT_TOLERANCE: f64 = 0.2;

/// Desired quote age beyond which the trade thread cancels but does not post.
/// Post-only orders cannot cross (they get rejected instead of taking), so
/// this is a churn guard rather than a slippage guard and can be generous.
pub const POST_FRESHNESS_MS: f64 = 250.0;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QuoteSide {
    pub price: f64,
    pub volume: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MakerDesire {
    pub fair_mid: f64,
    pub bid: Option<QuoteSide>,
    pub ask: Option<QuoteSide>,
    pub reason: &'static str,
}

/// Latest desired quotes for one pair, published by the evaluator.
#[derive(Clone, Debug)]
pub struct DesiredQuote {
    pub pair_name: &'static str,
    pub sibling_name: &'static str,
    pub fair_mid: f64,
    pub bid: Option<QuoteSide>,
    pub ask: Option<QuoteSide>,
    pub price_decimals: usize,
    pub volume_decimals: usize,
    pub inventory_coin: f64,
    pub reason: &'static str,
    pub opportunity_id: u64,
    pub eval_ts_ns: u128,
    /// Monotonic publish sequence; the reconciler tracks the last seq it saw.
    pub seq: u64,
    /// Set when live order state changed underneath this desire (fill or
    /// rejection): the next publish must not be coalesced away, even if the
    /// evaluator derives identical prices.
    pub dirty: bool,
}

/// Own resting orders per pair, mirrored by the trade thread so the evaluator
/// can subtract them from the public book (self-exclusion).
#[derive(Clone, Copy, Debug, Default)]
pub struct LiveQuotes {
    /// (price, remaining volume)
    pub bid: Option<(f64, f64)>,
    pub ask: Option<(f64, f64)>,
}

static DESIRED: OnceLock<Mutex<FxHashMap<&'static str, DesiredQuote>>> = OnceLock::new();
static DESIRED_SEQ: AtomicU64 = AtomicU64::new(1);
static WAKE: OnceLock<Notify> = OnceLock::new();
static INVENTORY: OnceLock<Mutex<FxHashMap<String, i64>>> = OnceLock::new();
static LIVE: OnceLock<Mutex<FxHashMap<&'static str, LiveQuotes>>> = OnceLock::new();

fn desired_map() -> &'static Mutex<FxHashMap<&'static str, DesiredQuote>> {
    DESIRED.get_or_init(|| Mutex::new(FxHashMap::default()))
}

fn inventory_map() -> &'static Mutex<FxHashMap<String, i64>> {
    INVENTORY.get_or_init(|| Mutex::new(FxHashMap::default()))
}

fn live_map() -> &'static Mutex<FxHashMap<&'static str, LiveQuotes>> {
    LIVE.get_or_init(|| Mutex::new(FxHashMap::default()))
}

/// Notifier that wakes the trade thread's maker reconciler.
pub fn maker_wake() -> &'static Notify {
    WAKE.get_or_init(Notify::new)
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

// ---------------------------------------------------------------------------
// Inventory (per pair, base coin units)
// ---------------------------------------------------------------------------

/// Maker-accumulated base inventory for `pair`, in coin units.
pub fn inventory_coin(pair: &str) -> f64 {
    let map = inventory_map().lock().unwrap();
    map.get(pair).copied().unwrap_or(0) as f64 / INV_SCALE
}

/// Apply a signed inventory delta from a fill (buy = +, sell = −).
pub fn apply_inventory_delta(pair: &str, delta_coin: f64) {
    let delta_e8 = (delta_coin * INV_SCALE).round() as i64;
    let mut map = inventory_map().lock().unwrap();
    *map.entry(pair.to_string()).or_insert(0) += delta_e8;
}

// ---------------------------------------------------------------------------
// Live-quote mirror (written by trade thread, read by evaluator)
// ---------------------------------------------------------------------------

pub fn set_live_bid(pair: &'static str, quote: Option<(f64, f64)>) {
    live_map().lock().unwrap().entry(pair).or_default().bid = quote;
}

pub fn set_live_ask(pair: &'static str, quote: Option<(f64, f64)>) {
    live_map().lock().unwrap().entry(pair).or_default().ask = quote;
}

pub fn live_quotes(pair: &str) -> LiveQuotes {
    live_map()
        .lock()
        .unwrap()
        .get(pair)
        .copied()
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Desired-quote registry
// ---------------------------------------------------------------------------

fn side_changed(old: &Option<QuoteSide>, new: &Option<QuoteSide>, tick: f64) -> bool {
    match (old, new) {
        (None, None) => false,
        (Some(o), Some(n)) => {
            (o.price - n.price).abs() >= tick * 0.5 || volume_drifted(o.volume, n.volume)
        }
        _ => true,
    }
}

/// True when the size difference is large enough to justify cancel/replace.
pub fn volume_drifted(live: f64, want: f64) -> bool {
    (live - want).abs() > VOLUME_DRIFT_TOLERANCE * live.max(want)
}

/// Store the latest desire for a pair. Returns true (and wakes the
/// reconciler) only when the desire materially changed — same-price,
/// similar-size republishes are dropped at the source to avoid churn.
/// A dirty entry (fill/reject since last publish) is never coalesced.
pub fn publish_desired(mut desire: DesiredQuote) -> bool {
    let tick = tick_size(desire.price_decimals);
    {
        let mut map = desired_map().lock().unwrap();
        match map.get(desire.pair_name) {
            Some(prev)
                if !prev.dirty
                    && !side_changed(&prev.bid, &desire.bid, tick)
                    && !side_changed(&prev.ask, &desire.ask, tick) =>
            {
                return false;
            }
            // Nothing ever published for this pair and nothing desired:
            // no live orders can exist, so there is nothing to reconcile.
            None if desire.bid.is_none() && desire.ask.is_none() => return false,
            _ => {}
        }
        desire.seq = DESIRED_SEQ.fetch_add(1, Ordering::Relaxed);
        desire.dirty = false;
        map.insert(desire.pair_name, desire);
    }
    maker_wake().notify_one();
    true
}

/// Publish cancel-desires for every pair with an active desire. Used when a
/// listener's books are resetting (reconnect / checksum failure).
pub fn publish_cancel_all(pair_names: &[&'static str], reason: &'static str) {
    let mut any = false;
    {
        let mut map = desired_map().lock().unwrap();
        for &pair in pair_names.iter().skip(2) {
            if let Some(entry) = map.get_mut(pair) {
                if entry.bid.is_some() || entry.ask.is_some() {
                    entry.bid = None;
                    entry.ask = None;
                    entry.reason = reason;
                    entry.eval_ts_ns = now_ns();
                    entry.seq = DESIRED_SEQ.fetch_add(1, Ordering::Relaxed);
                    any = true;
                }
            }
        }
    }
    if any {
        maker_wake().notify_one();
    }
}

/// Mark a pair's stored desire dirty so the evaluator's next publish is
/// never coalesced away. Called after fills and post rejections: live state
/// changed underneath the registry, so "unchanged desire" no longer implies
/// "nothing to do".
pub fn invalidate_pair(pair: &str) {
    let mut map = desired_map().lock().unwrap();
    if let Some(entry) = map.values_mut().find(|d| d.pair_name == pair) {
        entry.dirty = true;
    }
}

/// Snapshot every desire published since `last_seen`, oldest first.
pub fn drain_changed(last_seen: &mut u64) -> Vec<DesiredQuote> {
    let map = desired_map().lock().unwrap();
    let mut out: Vec<DesiredQuote> = map
        .values()
        .filter(|d| d.seq > *last_seen)
        .cloned()
        .collect();
    drop(map);
    out.sort_by_key(|d| d.seq);
    if let Some(max) = out.last().map(|d| d.seq) {
        *last_seen = max;
    }
    out
}

// ---------------------------------------------------------------------------
// Quote math
// ---------------------------------------------------------------------------

fn mid(bid: f64, ask: f64) -> Option<f64> {
    if bid <= 0.0 || ask <= 0.0 {
        return None;
    }
    Some((bid + ask) / 2.0)
}

/// Convert sibling mid into the target pair's quote currency via stable mids.
pub fn sibling_fair_mid(
    sibling: &PairData,
    target_stable: &PairData,
    sibling_stable: &PairData,
) -> Option<f64> {
    let sib_mid = mid(sibling.bid_price, sibling.ask_price)?;
    let tgt_fx = mid(target_stable.bid_price, target_stable.ask_price)?;
    let sib_fx = mid(sibling_stable.bid_price, sibling_stable.ask_price)?;
    if sib_fx <= 0.0 {
        return None;
    }
    Some(sib_mid * (tgt_fx / sib_fx))
}

pub fn tick_size(price_decimals: usize) -> f64 {
    10f64.powi(-(price_decimals as i32))
}

/// Quote offset actually used, in bps: the configured offset floored at the
/// maker fee plus a minimum edge, so a filled round trip is positive-EV by
/// construction (2×offset earned vs 2×fee paid).
pub fn effective_offset_bps() -> f64 {
    let configured = MAKER_OFFSET_BPS.load(Ordering::Relaxed).max(0) as f64;
    let floor = (FEE_MAKER.load(Ordering::Relaxed).max(0) + MAKER_MIN_EDGE_BPS) as f64;
    configured.max(floor)
}

/// Best bid/ask excluding our own resting orders. Walks book levels and skips
/// any level whose visible volume is entirely our own quote, so we never
/// price against (or one-up) ourselves.
pub fn effective_bbo(book: &OrderBook, live: &LiveQuotes, tick: f64) -> Option<(f64, f64)> {
    let own_volume = |own: &Option<(f64, f64)>, price: f64| -> f64 {
        match own {
            Some((p, v)) if (p - price).abs() < tick * 0.5 => *v,
            _ => 0.0,
        }
    };

    let mut eff_ask = None;
    for level in book.asks.iter().take(book.ask_count as usize) {
        if level.price <= 0.0 || level.volume <= 0.0 {
            continue;
        }
        if level.volume - own_volume(&live.ask, level.price) > 1e-12 {
            eff_ask = Some(level.price);
            break;
        }
    }

    let mut eff_bid = None;
    for level in book.bids.iter().take(book.bid_count as usize) {
        if level.price <= 0.0 || level.volume <= 0.0 {
            continue;
        }
        if level.volume - own_volume(&live.bid, level.price) > 1e-12 {
            eff_bid = Some(level.price);
            break;
        }
    }

    match (eff_bid, eff_ask) {
        (Some(b), Some(a)) => Some((b, a)),
        _ => None,
    }
}

/// Build inside-spread bid/ask around fair mid with per-side sizing:
/// - bid volume is capped by remaining notional headroom (long inventory
///   shrinks the bid until it disappears at the cap);
/// - ask volume is capped by held inventory (never sell base we don't have,
///   which would be rejected on spot).
///
/// Always returns a desire; an unquotable side is `None`, and both sides
/// `None` means "cancel".
pub fn desired_quotes(
    target: &PairData,
    eff_bid: f64,
    eff_ask: f64,
    fair_mid: f64,
    inventory_coin: f64,
    notional: f64,
    offset_bps: f64,
) -> MakerDesire {
    let cancel = |reason: &'static str| MakerDesire {
        fair_mid,
        bid: None,
        ask: None,
        reason,
    };

    if fair_mid <= 0.0 || eff_bid <= 0.0 || eff_ask <= 0.0 {
        return cancel("no_book");
    }
    if eff_ask <= eff_bid {
        return cancel("crossed_book");
    }
    let tick = tick_size(target.price_decimals);
    if tick <= 0.0 {
        return cancel("no_tick");
    }

    let offset = fair_mid * offset_bps / 10_000.0;
    let ideal_bid = fair_mid - offset;
    let ideal_ask = fair_mid + offset;

    // Prefer one-tick improve; else join the BBO if still on the fair side of
    // ideal. If fair is through the book (ideal below bid / above ask), skip
    // that side.
    let improve_bid = eff_bid + tick;
    let mut bid_price = if improve_bid <= ideal_bid && improve_bid < eff_ask {
        Some(improve_bid)
    } else if eff_bid <= ideal_bid {
        Some(eff_bid)
    } else {
        None
    };

    let improve_ask = eff_ask - tick;
    let mut ask_price = if improve_ask >= ideal_ask && improve_ask > eff_bid {
        Some(improve_ask)
    } else if eff_ask >= ideal_ask {
        Some(eff_ask)
    } else {
        None
    };

    if let (Some(b), Some(a)) = (bid_price, ask_price) {
        if b >= a {
            bid_price = None;
            ask_price = None;
        }
    }

    // Per-side sizing.
    let full_volume = notional / fair_mid;
    let inv_notional = inventory_coin * fair_mid;
    let bid_headroom_notional = (notional - inv_notional).clamp(0.0, notional);
    let bid_volume = bid_headroom_notional / fair_mid;
    let ask_volume = inventory_coin.clamp(0.0, full_volume);

    const FACTOR_OF_SAFETY: f64 = 1.01;
    let viable = |volume: f64, price: f64| -> bool {
        volume >= target.order_min
            && (target.cost_min <= 0.0 || volume * price >= target.cost_min * FACTOR_OF_SAFETY)
    };

    let bid = bid_price
        .filter(|&p| viable(bid_volume, p))
        .map(|price| QuoteSide {
            price,
            volume: bid_volume,
        });
    let ask = ask_price
        .filter(|&p| viable(ask_volume, p))
        .map(|price| QuoteSide {
            price,
            volume: ask_volume,
        });

    let reason = match (&bid, &ask) {
        (Some(_), Some(_)) => "two_sided",
        (Some(_), None) => "bid_only",
        (None, Some(_)) => "ask_only",
        (None, None) => "no_room_or_capped",
    };

    MakerDesire {
        fair_mid,
        bid,
        ask,
        reason,
    }
}

/// Evaluate the full desired state for one pair. Returns a cancel-desire
/// (both sides `None`) whenever the data needed to price safely is missing,
/// so stale quotes get pulled instead of resting on bad data.
#[allow(clippy::too_many_arguments)]
pub fn build_desired(
    target: &PairData,
    sibling: &PairData,
    target_stable: &PairData,
    sibling_stable: &PairData,
    target_name: &'static str,
    sibling_name: &'static str,
    target_book: Option<&OrderBook>,
) -> DesiredQuote {
    let inventory = inventory_coin(target_name);
    let base = |fair_mid: f64, reason: &'static str| DesiredQuote {
        pair_name: target_name,
        sibling_name,
        fair_mid,
        bid: None,
        ask: None,
        price_decimals: target.price_decimals,
        volume_decimals: target.volume_decimals,
        inventory_coin: inventory,
        reason,
        opportunity_id: next_opportunity_id(),
        eval_ts_ns: now_ns(),
        seq: 0,
        dirty: false,
    };

    if !target.pair_status
        || !sibling.pair_status
        || !target_stable.pair_status
        || !sibling_stable.pair_status
    {
        return base(0.0, "pair_offline");
    }
    let Some(fair_mid) = sibling_fair_mid(sibling, target_stable, sibling_stable) else {
        return base(0.0, "no_fair");
    };
    let Some(book) = target_book.filter(|b| b.ready) else {
        return base(fair_mid, "book_not_ready");
    };
    let tick = tick_size(target.price_decimals);
    let live = live_quotes(target_name);
    let Some((eff_bid, eff_ask)) = effective_bbo(book, &live, tick) else {
        return base(fair_mid, "no_effective_bbo");
    };

    let notional = MAKER_NOTIONAL.load(Ordering::Relaxed).max(1) as f64;
    let desire = desired_quotes(
        target,
        eff_bid,
        eff_ask,
        fair_mid,
        inventory,
        notional,
        effective_offset_bps(),
    );

    let mut out = base(desire.fair_mid, desire.reason);
    out.bid = desire.bid;
    out.ask = desire.ask;
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(bid: f64, ask: f64, decimals: usize) -> PairData {
        PairData {
            bid_price: bid,
            ask_price: ask,
            bid_volume: 10.0,
            ask_volume: 10.0,
            order_min: 0.001,
            cost_min: 0.0,
            price_decimals: decimals,
            volume_decimals: 8,
            pair_status: true,
            kraken_ts: 1.0,
        }
    }

    fn stable(bid: f64, ask: f64) -> PairData {
        pair(bid, ask, 4)
    }

    #[test]
    fn fair_mid_applies_fx() {
        // Sibling mid 100 EUR; FX EUR→USD = 1.2 → fair 120 USD.
        let sibling = pair(99.0, 101.0, 2);
        let fair =
            sibling_fair_mid(&sibling, &stable(1.2, 1.2), &stable(1.0, 1.0)).unwrap();
        assert!((fair - 120.0).abs() < 1e-9);
    }

    #[test]
    fn quotes_inside_spread_with_offset() {
        let target = pair(100.0, 101.0, 2); // tick 0.01
        let fair = 100.5;
        // Inventory present so the ask side is sellable.
        let d = desired_quotes(&target, 100.0, 101.0, fair, 0.05, 10.0, 30.0);
        let bid = d.bid.expect("bid");
        let ask = d.ask.expect("ask");
        // offset = 100.5 * 30 / 10000 ≈ 0.30 → ideal bid ≈ 100.20, ideal ask ≈ 100.80.
        assert!(bid.price > 100.0 && bid.price < ask.price);
        assert!(ask.price < 101.0);
        assert_eq!(d.reason, "two_sided");
    }

    #[test]
    fn ask_requires_inventory() {
        let target = pair(100.0, 101.0, 2);
        let d = desired_quotes(&target, 100.0, 101.0, 100.5, 0.0, 10.0, 30.0);
        assert!(d.bid.is_some());
        assert!(d.ask.is_none(), "cannot sell base we do not hold");
        assert_eq!(d.reason, "bid_only");
    }

    #[test]
    fn ask_volume_capped_by_inventory() {
        let target = pair(100.0, 101.0, 2);
        let inv = 0.05; // held base < full size (10/100.5 ≈ 0.0995)
        let d = desired_quotes(&target, 100.0, 101.0, 100.5, inv, 10.0, 30.0);
        let ask = d.ask.expect("ask");
        assert!((ask.volume - inv).abs() < 1e-12);
    }

    #[test]
    fn suppresses_bid_when_inventory_long_capped() {
        let target = pair(100.0, 101.0, 2);
        let fair = 100.5;
        let inv = 10.0 / fair; // exactly at notional
        let d = desired_quotes(&target, 100.0, 101.0, fair, inv, 10.0, 30.0);
        assert!(d.bid.is_none());
        assert!(d.ask.is_some());
        assert_eq!(d.reason, "ask_only");
    }

    #[test]
    fn bid_shrinks_with_inventory_headroom() {
        let target = pair(100.0, 101.0, 2);
        let fair = 100.5;
        let inv = 5.0 / fair; // half the notional already held
        let d = desired_quotes(&target, 100.0, 101.0, fair, inv, 10.0, 30.0);
        let bid = d.bid.expect("bid");
        assert!((bid.volume - 5.0 / fair).abs() < 1e-9);
    }

    #[test]
    fn no_bid_when_below_order_min() {
        let mut target = pair(100.0, 101.0, 2);
        target.order_min = 1.0;
        // notional 10 / mid ~0.1 << order_min 1
        let d = desired_quotes(&target, 100.0, 101.0, 100.5, 0.0, 10.0, 30.0);
        assert!(d.bid.is_none() && d.ask.is_none());
        assert_eq!(d.reason, "no_room_or_capped");
    }

    #[test]
    fn tight_spread_yields_no_room_when_fair_offset_wide() {
        // One-tick spread + wide offset: ideal is through the BBO → no quote.
        let target = pair(100.00, 100.01, 2);
        let d = desired_quotes(&target, 100.00, 100.01, 100.005, 0.0, 10.0, 30.0);
        assert!(d.bid.is_none());
        assert!(d.ask.is_none());
    }

    #[test]
    fn effective_bbo_excludes_own_quotes() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[("101.00", "0.10", 1.0), ("101.10", "5.0", 1.0)],
            &[("100.00", "0.10", 1.0), ("99.90", "5.0", 1.0)],
        );
        let tick = tick_size(2);

        // No own orders: BBO is the raw book.
        let (b, a) = effective_bbo(&book, &LiveQuotes::default(), tick).unwrap();
        assert!((b - 100.0).abs() < 1e-9 && (a - 101.0).abs() < 1e-9);

        // Own orders make up the entire top of book on both sides.
        let live = LiveQuotes {
            bid: Some((100.0, 0.10)),
            ask: Some((101.0, 0.10)),
        };
        let (b, a) = effective_bbo(&book, &live, tick).unwrap();
        assert!((b - 99.90).abs() < 1e-9, "own bid excluded");
        assert!((a - 101.10).abs() < 1e-9, "own ask excluded");

        // Own order is only part of the level: level still stands.
        let live = LiveQuotes {
            bid: Some((100.0, 0.05)),
            ask: None,
        };
        let (b, _) = effective_bbo(&book, &live, tick).unwrap();
        assert!((b - 100.0).abs() < 1e-9);
    }

    #[test]
    fn offset_floored_at_fee_plus_edge() {
        // Statics default: MAKER_OFFSET_BPS = 5, FEE_MAKER = 25 → floor 27.
        let offset = effective_offset_bps();
        let floor =
            (FEE_MAKER.load(Ordering::Relaxed) + MAKER_MIN_EDGE_BPS) as f64;
        assert!(offset >= floor);
    }

    #[test]
    fn volume_drift_tolerance() {
        assert!(!volume_drifted(1.0, 0.9));
        assert!(!volume_drifted(1.0, 1.1));
        assert!(volume_drifted(1.0, 0.7));
        assert!(volume_drifted(1.0, 1.5));
        assert!(volume_drifted(1.0, 0.0));
    }

    #[test]
    fn publish_coalesces_unchanged_desires() {
        let desire = DesiredQuote {
            pair_name: "TEST_PUBLISH/USD",
            sibling_name: "TEST_PUBLISH/EUR",
            fair_mid: 100.0,
            bid: Some(QuoteSide {
                price: 99.9,
                volume: 0.1,
            }),
            ask: None,
            price_decimals: 2,
            volume_decimals: 8,
            inventory_coin: 0.0,
            reason: "bid_only",
            opportunity_id: 1,
            eval_ts_ns: 0,
            seq: 0,
            dirty: false,
        };
        assert!(publish_desired(desire.clone()), "first publish is a change");
        assert!(
            !publish_desired(desire.clone()),
            "identical republish coalesced"
        );

        let mut moved = desire.clone();
        moved.bid = Some(QuoteSide {
            price: 99.95,
            volume: 0.1,
        });
        assert!(publish_desired(moved), "price move republished");

        let mut last_seen = 0u64;
        let drained = drain_changed(&mut last_seen);
        assert!(drained.iter().any(|d| d.pair_name == "TEST_PUBLISH/USD"));
        assert!(drain_changed(&mut last_seen)
            .iter()
            .all(|d| d.pair_name != "TEST_PUBLISH/USD"));
    }

    #[test]
    fn invalidate_forces_republish() {
        let desire = DesiredQuote {
            pair_name: "TEST_DIRTY/USD",
            sibling_name: "TEST_DIRTY/EUR",
            fair_mid: 100.0,
            bid: Some(QuoteSide {
                price: 99.9,
                volume: 0.1,
            }),
            ask: None,
            price_decimals: 2,
            volume_decimals: 8,
            inventory_coin: 0.0,
            reason: "bid_only",
            opportunity_id: 1,
            eval_ts_ns: 0,
            seq: 0,
            dirty: false,
        };
        assert!(publish_desired(desire.clone()));
        assert!(!publish_desired(desire.clone()), "coalesced when clean");
        invalidate_pair("TEST_DIRTY/USD");
        assert!(
            publish_desired(desire.clone()),
            "identical desire republished after invalidation"
        );
        assert!(!publish_desired(desire), "dirty flag cleared by publish");
    }

    #[test]
    fn inventory_roundtrip() {
        apply_inventory_delta("TEST_INV/USD", 0.5);
        apply_inventory_delta("TEST_INV/USD", -0.2);
        assert!((inventory_coin("TEST_INV/USD") - 0.3).abs() < 1e-9);
        assert_eq!(inventory_coin("TEST_INV_OTHER/USD"), 0.0);
    }
}
