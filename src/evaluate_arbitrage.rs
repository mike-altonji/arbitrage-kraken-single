use crate::structs::PairDataVec;
use crate::{EUR_BALANCE, FEE_SPOT, FEE_STABLECOIN, USD_BALANCE};

pub fn evaluate_arbitrage(pair_data_vec: &PairDataVec, idx: usize) {
    // Atomically read the fee and balance values
    let fee_spot = FEE_SPOT.load(std::sync::atomic::Ordering::Relaxed);
    let fee_stablecoin = FEE_STABLECOIN.load(std::sync::atomic::Ordering::Relaxed);
    let usd_balance = USD_BALANCE.load(std::sync::atomic::Ordering::Relaxed);
    let eur_balance = EUR_BALANCE.load(std::sync::atomic::Ordering::Relaxed);

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
}
