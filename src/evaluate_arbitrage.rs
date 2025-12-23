use crate::structs::PairDataVec;
use crate::{EUR_BALANCE, FEE_SPOT, FEE_STABLECOIN, USD_BALANCE};

pub fn evaluate_arbitrage(pair_data_vec: &PairDataVec, idx: usize) {
    let usd_pair = pair_data_vec.get(idx - (idx % 2));
    let eur_pair = pair_data_vec.get(idx + 1 - (idx % 2));
    let usd_stable_pair = pair_data_vec.get(0);
    let eur_stable_pair = pair_data_vec.get(1);
    if usd_pair.is_none()
        || eur_pair.is_none()
        || usd_stable_pair.is_none()
        || eur_stable_pair.is_none()
    {
        log::error!("Failed to get pair for index {} or stablecoin", idx);
        return;
    }
    let usd_pair = usd_pair.unwrap();
    let eur_pair = eur_pair.unwrap();
    let usd_stable_pair = usd_stable_pair.unwrap();
    let eur_stable_pair = eur_stable_pair.unwrap();

    // Skip if any pair is offline
    if !usd_pair.pair_status
        || !eur_pair.pair_status
        || !usd_stable_pair.pair_status
        || !eur_stable_pair.pair_status
    {
        return;
    }

    // Skip if no price data (usually during initialization)
    if usd_pair.bid_price == 0.0
        || usd_pair.ask_price == 0.0
        || eur_pair.bid_price == 0.0
        || eur_pair.ask_price == 0.0
    {
        return;
    }
    if usd_stable_pair.bid_price == 0.0
        || usd_stable_pair.ask_price == 0.0
        || eur_stable_pair.bid_price == 0.0
        || eur_stable_pair.ask_price == 0.0
    {
        return;
    }

    // Atomically read fees
    let fee_spot = FEE_SPOT.load(std::sync::atomic::Ordering::Relaxed) as f64;
    let fee_stablecoin = FEE_STABLECOIN.load(std::sync::atomic::Ordering::Relaxed) as f64;

    // Calculate arbitrage opportunity
    // The math on these formulas works out. Splitting to avoid double computation.
    let arb_fee = ((10_000.0 - fee_spot) * (10_000.0 - fee_stablecoin))
        / ((10_000.0 + fee_spot) * (10_000.0 + fee_stablecoin));
    let arb_prices_usd_start = (eur_pair.bid_price * usd_stable_pair.bid_price)
        / (usd_pair.ask_price * eur_stable_pair.ask_price);
    let arb_prices_eur_start = (usd_pair.bid_price * eur_stable_pair.bid_price)
        / (eur_pair.ask_price * usd_stable_pair.ask_price);

    let arb_roi_usd_start = arb_prices_usd_start * arb_fee;
    let arb_roi_eur_start = arb_prices_eur_start * arb_fee;

    // Atomically read balances
    let usd_balance = USD_BALANCE.load(std::sync::atomic::Ordering::Relaxed);
    let eur_balance = EUR_BALANCE.load(std::sync::atomic::Ordering::Relaxed);
}
