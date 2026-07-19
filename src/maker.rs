//! Maker mode (`--maker`): resting post-only quotes sized from sibling-pair FX fair value.
//!
//! When enabled, arb and momentum are disabled. Quotes lean inside the target
//! pair's spread around the sibling-converted fair mid, with a hard notional
//! and inventory cap. Cancel/replace is the edge — execution lives in `trade.rs`.

use crate::structs::{MakerAction, PairData};
use crate::{
    MAKER_INV_COIN_E8, MAKER_NOTIONAL, MAKER_OFFSET_BPS, FEE_MAKER_BPS,
};
use crate::arb_forensics::next_opportunity_id;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

/// Scale factor for the shared inventory atomic (coin units × 1e8).
pub const INV_SCALE: f64 = 100_000_000.0;

#[derive(Clone, Debug, PartialEq)]
pub struct MakerDesire {
    pub fair_mid: f64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub volume_coin: f64,
    pub reason: &'static str,
}

fn mid(bid: f64, ask: f64) -> Option<f64> {
    if bid <= 0.0 || ask <= 0.0 {
        return None;
    }
    Some((bid + ask) / 2.0)
}

/// Convert sibling mid into the target pair's quote currency via stable mids.
pub fn sibling_fair_mid(
    target: &PairData,
    sibling: &PairData,
    target_stable: &PairData,
    sibling_stable: &PairData,
) -> Option<f64> {
    let _ = target; // target book used by desired_quotes; fair uses sibling + FX
    let sib_mid = mid(sibling.bid_price, sibling.ask_price)?;
    let tgt_fx = mid(target_stable.bid_price, target_stable.ask_price)?;
    let sib_fx = mid(sibling_stable.bid_price, sibling_stable.ask_price)?;
    if sib_fx <= 0.0 {
        return None;
    }
    Some(sib_mid * (tgt_fx / sib_fx))
}

fn tick_size(price_decimals: usize) -> f64 {
    10f64.powi(-(price_decimals as i32))
}

/// Build inside-spread bid/ask around fair mid, gated by inventory and notional.
pub fn desired_quotes(
    target: &PairData,
    fair_mid: f64,
    inventory_coin: f64,
    notional: f64,
    offset_bps: i16,
) -> Option<MakerDesire> {
    if fair_mid <= 0.0 || target.bid_price <= 0.0 || target.ask_price <= 0.0 {
        return None;
    }
    if target.ask_price <= target.bid_price {
        return None;
    }

    let tick = tick_size(target.price_decimals);
    if tick <= 0.0 {
        return None;
    }

    let offset = fair_mid * (offset_bps.max(0) as f64) / 10_000.0;
    let ideal_bid = fair_mid - offset;
    let ideal_ask = fair_mid + offset;

    // Prefer one-tick improve; else join the BBO if still on the fair side of ideal.
    // If fair is through the book (ideal below bid / above ask), skip that side.
    let improve_bid = target.bid_price + tick;
    let mut bid_price = if improve_bid <= ideal_bid && improve_bid < target.ask_price {
        Some(improve_bid)
    } else if target.bid_price <= ideal_bid && target.bid_price < target.ask_price {
        Some(target.bid_price)
    } else {
        None
    };

    let improve_ask = target.ask_price - tick;
    let mut ask_price = if improve_ask >= ideal_ask && improve_ask > target.bid_price {
        Some(improve_ask)
    } else if target.ask_price >= ideal_ask && target.ask_price > target.bid_price {
        Some(target.ask_price)
    } else {
        None
    };

    if let (Some(b), Some(a)) = (bid_price, ask_price) {
        if b >= a {
            bid_price = None;
            ask_price = None;
        }
    }

    let inv_notional = inventory_coin * fair_mid;

    // Hard inventory notional cap: only quote the reducing side when capped.
    if inv_notional >= notional {
        bid_price = None;
    }
    if inv_notional <= -notional {
        ask_price = None;
    }

    if bid_price.is_none() && ask_price.is_none() {
        return Some(MakerDesire {
            fair_mid,
            bid_price: None,
            ask_price: None,
            volume_coin: 0.0,
            reason: "no_room_or_inventory_capped",
        });
    }

    let volume = notional / fair_mid;
    const FACTOR_OF_SAFETY: f64 = 1.01;
    if volume < target.order_min
        || (target.cost_min > 0.0 && volume * fair_mid < target.cost_min * FACTOR_OF_SAFETY)
    {
        return None;
    }

    let reason = if bid_price.is_some() && ask_price.is_some() {
        "two_sided"
    } else if bid_price.is_some() {
        "bid_only"
    } else {
        "ask_only"
    };

    let _ = FEE_MAKER_BPS; // documented assumption; offset is the EV buffer

    Some(MakerDesire {
        fair_mid,
        bid_price,
        ask_price,
        volume_coin: volume,
        reason,
    })
}

/// Read shared inventory (coin units).
pub fn inventory_coin() -> f64 {
    MAKER_INV_COIN_E8.load(Ordering::Relaxed) as f64 / INV_SCALE
}

/// Apply a signed inventory delta from a fill (buy = +, sell = −).
pub fn apply_inventory_delta(delta_coin: f64) {
    let delta_e8 = (delta_coin * INV_SCALE).round() as i64;
    MAKER_INV_COIN_E8.fetch_add(delta_e8, Ordering::Relaxed);
}

/// Evaluate maker desire for the updated pair and build a trade-channel action.
#[allow(clippy::too_many_arguments)]
pub fn maybe_maker_action(
    target: &PairData,
    sibling: &PairData,
    target_stable: &PairData,
    sibling_stable: &PairData,
    target_name: &'static str,
    sibling_name: &'static str,
) -> Option<MakerAction> {
    let fair_mid = sibling_fair_mid(target, sibling, target_stable, sibling_stable)?;
    let notional = MAKER_NOTIONAL.load(Ordering::Relaxed).max(1) as f64;
    let offset_bps = MAKER_OFFSET_BPS.load(Ordering::Relaxed);
    let inv = inventory_coin();
    let desire = desired_quotes(target, fair_mid, inv, notional, offset_bps)?;

    let send_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    Some(MakerAction {
        pair_name: target_name,
        sibling_name,
        fair_mid: desire.fair_mid,
        bid_price: desire.bid_price,
        ask_price: desire.ask_price,
        volume_coin: desire.volume_coin,
        price_decimals: target.price_decimals,
        volume_decimals: target.volume_decimals,
        inventory_coin: inv,
        send_timestamp,
        updated_pair_kraken_ts: target.kraken_ts,
        opportunity_id: next_opportunity_id(),
        reason: desire.reason,
    })
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
        let target = pair(119.0, 121.0, 2);
        let sibling = pair(99.0, 101.0, 2);
        let fair = sibling_fair_mid(
            &target,
            &sibling,
            &stable(1.2, 1.2),
            &stable(1.0, 1.0),
        )
        .unwrap();
        assert!((fair - 120.0).abs() < 1e-9);
    }

    #[test]
    fn quotes_inside_spread_with_offset() {
        let target = pair(100.0, 101.0, 2); // tick 0.01
        let fair = 100.5;
        let d = desired_quotes(&target, fair, 0.0, 10.0, 5).expect("quotes");
        // offset = 100.5 * 5 / 10000 = 0.05025
        // ideal bid 100.44975 → min with bid+tick=100.01 → 100.01
        // ideal ask 100.55025 → max with ask-tick=100.99 → 100.99
        assert!(d.bid_price.unwrap() < d.ask_price.unwrap());
        assert!(d.bid_price.unwrap() > target.bid_price);
        assert!(d.ask_price.unwrap() < target.ask_price);
        assert_eq!(d.reason, "two_sided");
    }

    #[test]
    fn suppresses_bid_when_inventory_long_capped() {
        let target = pair(100.0, 101.0, 2);
        let fair = 100.5;
        let inv = 10.0 / fair; // exactly at notional
        let d = desired_quotes(&target, fair, inv, 10.0, 5).expect("quotes");
        assert!(d.bid_price.is_none());
        assert!(d.ask_price.is_some());
        assert_eq!(d.reason, "ask_only");
    }

    #[test]
    fn returns_none_volume_when_below_order_min() {
        let mut target = pair(100.0, 101.0, 2);
        target.order_min = 1.0;
        // notional 10 / mid ~0.1 << order_min 1
        assert!(desired_quotes(&target, 100.5, 0.0, 10.0, 5).is_none());
    }

    #[test]
    fn volume_from_notional() {
        let target = pair(100.0, 101.0, 2);
        let d = desired_quotes(&target, 100.0, 0.0, 10.0, 5).unwrap();
        assert!((d.volume_coin - 0.1).abs() < 1e-12);
    }

    #[test]
    fn tight_spread_yields_no_room_when_fair_offset_wide() {
        // One-tick spread + 5bps offset: ideal is through the BBO → no quote.
        let target = pair(100.00, 100.01, 2);
        let d = desired_quotes(&target, 100.005, 0.0, 10.0, 5).unwrap();
        assert!(d.bid_price.is_none());
        assert!(d.ask_price.is_none());
        assert_eq!(d.reason, "no_room_or_inventory_capped");
    }
}
