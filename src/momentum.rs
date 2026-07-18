//! Momentum trade mode (`--momentum`): same-pair round trip triggered by the
//! sibling pair's bid jumping.
//!
//! When pair X's bid improves and X's fx-converted price is far enough above
//! pair Y's ask that Y converging halfway would cover both spot fees, we buy Y
//! at its ask (limit IOC), hold for a log-uniformly sampled 1–1000 ms, then
//! market sell on Y — betting Y follows X upward. Directional and experimental:
//! there is no ROI > 1 guarantee at entry.

use crate::arb_forensics::next_opportunity_id;
use crate::structs::{MomentumOrder, PairData};
use std::time::{SystemTime, UNIX_EPOCH};

/// Fraction of the X–Y gap that Y is assumed to close before we exit.
const CONVERGENCE_FRACTION: f64 = 0.5;

/// Sample a hold time log-uniformly in [1, 1000] ms: 10^uniform(0, 3).
pub fn sample_hold_ms() -> f64 {
    let exp = rand::random::<f64>() * 3.0;
    10f64.powf(exp)
}

/// Evaluate the momentum entry gate for a `bid_improved` event on `x_pair`.
///
/// `y_pair` is the sibling we round-trip. Returns an order when the halfway
/// convergence estimate covers both spot fees and sizing passes guardrails.
#[allow(clippy::too_many_arguments)]
pub fn maybe_momentum_order(
    x_pair: &PairData,
    y_pair: &PairData,
    x_stable: &PairData,
    y_stable: &PairData,
    x_pair_name: &'static str,
    y_pair_name: &'static str,
    balance: f64,
    fee_spot: f64,
) -> Option<MomentumOrder> {
    if x_pair.bid_price <= 0.0 || y_pair.ask_price <= 0.0 || y_pair.ask_volume <= 0.0 {
        return None;
    }
    if x_stable.ask_price <= 0.0 || y_stable.bid_price <= 0.0 {
        return None;
    }

    // Convert X's bid into Y's quote currency (same stable-leg math as arb ROI).
    let fx = y_stable.bid_price / x_stable.ask_price;
    let p_other = x_pair.bid_price * fx;

    let gap = p_other - y_pair.ask_price;
    if gap <= 0.0 {
        return None;
    }

    // If Y converges halfway to X's converted price, selling there must cover
    // both spot fees. Optimistic by roughly Y's spread (actual exit is a bid).
    let exit_est = y_pair.ask_price + CONVERGENCE_FRACTION * gap;
    if exit_est * (1.0 - fee_spot) < y_pair.ask_price * (1.0 + fee_spot) {
        return None;
    }

    let max_from_balance = balance / (y_pair.ask_price * (1.0 + fee_spot));
    let volume = y_pair.ask_volume.min(max_from_balance);
    if volume <= 0.0 {
        return None;
    }

    // Same guardrails as arb (single pair, so only Y's minimums apply).
    const FACTOR_OF_SAFETY: f64 = 1.01;
    if volume < y_pair.order_min
        || volume < y_pair.cost_min * y_pair.ask_price * FACTOR_OF_SAFETY
    {
        return None;
    }

    let gap_bps = gap / y_pair.ask_price * 10_000.0;
    let send_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    Some(MomentumOrder {
        pair_name: y_pair_name,
        trigger_pair_name: x_pair_name,
        volume_coin: volume,
        volume_decimals_coin: y_pair.volume_decimals,
        limit_buy_price: y_pair.ask_price,
        price_decimals: y_pair.price_decimals,
        hold_ms: sample_hold_ms(),
        gap_bps,
        send_timestamp,
        updated_pair_kraken_ts: x_pair.kraken_ts,
        opportunity_id: next_opportunity_id(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(bid: f64, ask: f64, ask_vol: f64) -> PairData {
        PairData {
            bid_price: bid,
            ask_price: ask,
            bid_volume: 1.0,
            ask_volume: ask_vol,
            order_min: 0.001,
            cost_min: 0.0,
            price_decimals: 2,
            volume_decimals: 8,
            pair_status: true,
            kraken_ts: 42.0,
        }
    }

    fn stable(bid: f64, ask: f64) -> PairData {
        pair(bid, ask, 1_000_000.0)
    }

    const FEE: f64 = 0.004;

    #[test]
    fn enters_when_half_gap_covers_fees() {
        // Y ask = 100; fees round trip ~0.8% => need exit_est >= ~100.803.
        // X bid = 102 (fx 1.0) => gap 2, exit_est 101 => enter.
        let x = pair(102.0, 103.0, 1.0);
        let y = pair(99.0, 100.0, 5.0);
        let order = maybe_momentum_order(
            &x,
            &y,
            &stable(1.0, 1.0),
            &stable(1.0, 1.0),
            "X/EUR",
            "X/USD",
            1_000_000.0,
            FEE,
        )
        .expect("should enter");
        assert_eq!(order.pair_name, "X/USD");
        assert_eq!(order.trigger_pair_name, "X/EUR");
        assert!((order.limit_buy_price - 100.0).abs() < 1e-9);
        assert!((order.volume_coin - 5.0).abs() < 1e-9);
        assert!((order.gap_bps - 200.0).abs() < 1e-6);
        assert!((1.0..=1000.0).contains(&order.hold_ms));
        assert_eq!(order.updated_pair_kraken_ts, 42.0);
    }

    #[test]
    fn declines_when_half_gap_below_fees() {
        // gap 1 => exit_est 100.5; 100.5*0.996 = 100.098 < 100*1.004 = 100.4.
        let x = pair(101.0, 102.0, 1.0);
        let y = pair(99.0, 100.0, 5.0);
        assert!(maybe_momentum_order(
            &x,
            &y,
            &stable(1.0, 1.0),
            &stable(1.0, 1.0),
            "X/EUR",
            "X/USD",
            1_000_000.0,
            FEE,
        )
        .is_none());
    }

    #[test]
    fn declines_on_negative_gap() {
        let x = pair(99.0, 100.0, 1.0);
        let y = pair(99.5, 100.5, 5.0);
        assert!(maybe_momentum_order(
            &x,
            &y,
            &stable(1.0, 1.0),
            &stable(1.0, 1.0),
            "X/EUR",
            "X/USD",
            1_000_000.0,
            FEE,
        )
        .is_none());
    }

    #[test]
    fn fx_conversion_applied_to_trigger_bid() {
        // X bid 90 in EUR terms, fx = 1.2/1.0 => 108 in USD terms vs Y ask 100.
        let x = pair(90.0, 91.0, 1.0);
        let y = pair(99.0, 100.0, 5.0);
        let order = maybe_momentum_order(
            &x,
            &y,
            &stable(1.0, 1.0), // X stable: ask 1.0
            &stable(1.2, 1.3), // Y stable: bid 1.2
            "X/EUR",
            "X/USD",
            1_000_000.0,
            FEE,
        )
        .expect("fx gap should enter");
        assert!((order.gap_bps - 800.0).abs() < 1e-6);
    }

    #[test]
    fn balance_caps_volume() {
        let x = pair(102.0, 103.0, 1.0);
        let y = pair(99.0, 100.0, 5.0);
        let balance = 200.0;
        let order = maybe_momentum_order(
            &x,
            &y,
            &stable(1.0, 1.0),
            &stable(1.0, 1.0),
            "X/EUR",
            "X/USD",
            balance,
            FEE,
        )
        .expect("should enter");
        let expected = balance / (100.0 * (1.0 + FEE));
        assert!((order.volume_coin - expected).abs() < 1e-9);
    }

    #[test]
    fn declines_below_min_order() {
        let x = pair(102.0, 103.0, 1.0);
        let mut y = pair(99.0, 100.0, 0.0001);
        y.order_min = 0.001;
        assert!(maybe_momentum_order(
            &x,
            &y,
            &stable(1.0, 1.0),
            &stable(1.0, 1.0),
            "X/EUR",
            "X/USD",
            1_000_000.0,
            FEE,
        )
        .is_none());
    }

    #[test]
    fn hold_ms_sampler_stays_in_range() {
        for _ in 0..10_000 {
            let h = sample_hold_ms();
            assert!((1.0..=1000.0).contains(&h), "hold_ms out of range: {}", h);
        }
    }
}
