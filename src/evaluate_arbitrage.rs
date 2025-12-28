use crate::influx::log_arbitrage_opportunity;
use crate::structs::{OrderInfo, PairData, PairDataVec};
use crate::{EUR_BALANCE, FEE_SPOT, FEE_STABLECOIN, TRADER_BUSY, USD_BALANCE};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

pub fn evaluate_arbitrage(
    pair_data_vec: &PairDataVec,
    idx: usize,
    pair_names: &[&'static str],
    trade_tx: mpsc::Sender<OrderInfo>,
) {
    let usd_pair_idx = idx - (idx % 2);
    let eur_pair_idx = idx + 1 - (idx % 2);
    let usd_pair = pair_data_vec.get(usd_pair_idx);
    let eur_pair = pair_data_vec.get(eur_pair_idx);
    let usd_stable_pair = pair_data_vec.get(0);
    let eur_stable_pair = pair_data_vec.get(1);
    let (usd_pair, eur_pair, usd_stable_pair, eur_stable_pair) =
        match (usd_pair, eur_pair, usd_stable_pair, eur_stable_pair) {
            (Some(usd), Some(eur), Some(usd_s), Some(eur_s)) => (usd, eur, usd_s, eur_s),
            _ => {
                log::error!("Failed to get pair for index {} or stablecoin", idx);
                return;
            }
        };

    // Skip if any pair is offline
    if !usd_pair.pair_status
        || !eur_pair.pair_status
        || !usd_stable_pair.pair_status
        || !eur_stable_pair.pair_status
    {
        // Not logging because it could be very noisy.
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
    let fee_spot = fee_spot / 10_000.0;
    let fee_stablecoin = fee_stablecoin / 10_000.0;

    // Calculate arbitrage opportunity
    // let arb_fee =
    //     ((1.0 - fee_spot) * (1.0 - fee_stablecoin)) / ((1.0 + fee_spot) * (1.0 + fee_stablecoin));
    let arb_fee = (1.0 - fee_spot) / (1.0 + fee_spot); // Remove stablecoin fees because we don't trade back

    // Check USD -> EUR arbitrage opportunity
    let arb_roi_usd = compute_roi(
        usd_pair,
        eur_pair,
        usd_stable_pair,
        eur_stable_pair,
        arb_fee,
    );
    if arb_roi_usd > 1.0 {
        process_arbitrage_opportunity(
            arb_roi_usd,
            usd_pair,
            eur_pair,
            usd_stable_pair,
            eur_stable_pair,
            USD_BALANCE.load(std::sync::atomic::Ordering::Relaxed) as f64,
            usd_pair_idx,
            eur_pair_idx,
            0,
            1,
            fee_spot,
            fee_stablecoin,
            pair_names,
            trade_tx.clone(),
        );
    }

    // Check EUR -> USD arbitrage opportunity
    let arb_roi_eur = compute_roi(
        eur_pair,
        usd_pair,
        eur_stable_pair,
        usd_stable_pair,
        arb_fee,
    );
    if arb_roi_eur > 1.0 {
        process_arbitrage_opportunity(
            arb_roi_eur,
            eur_pair,
            usd_pair,
            eur_stable_pair,
            usd_stable_pair,
            EUR_BALANCE.load(std::sync::atomic::Ordering::Relaxed) as f64,
            eur_pair_idx,
            usd_pair_idx,
            1,
            0,
            fee_spot,
            fee_stablecoin,
            pair_names,
            trade_tx.clone(),
        );
    }
}

/// Process an arbitrage opportunity: check volumes, guardrails, and trigger trades
fn process_arbitrage_opportunity(
    roi: f64,
    pair1: &PairData,
    pair2: &PairData,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    balance: f64,
    pair1_idx: usize,
    pair2_idx: usize,
    pair1_stable_idx: usize,
    pair2_stable_idx: usize,
    fee_spot: f64,
    fee_stablecoin: f64,
    pair_names: &[&'static str],
    trade_tx: mpsc::Sender<OrderInfo>,
) {
    // Get pair names safely - return early if any are missing
    let pair1_name = pair_names.get(pair1_idx).copied();
    let pair2_name = pair_names.get(pair2_idx).copied();
    let pair1_stable_name = pair_names.get(pair1_stable_idx).copied();
    let pair2_stable_name = pair_names.get(pair2_stable_idx).copied();

    let (pair1_name, pair2_name, pair1_stable_name, pair2_stable_name) =
        match (pair1_name, pair2_name, pair1_stable_name, pair2_stable_name) {
            (Some(p1), Some(p2), Some(p1s), Some(p2s)) => (p1, p2, p1s, p2s),
            _ => {
                log::error!(
                    "Failed to get pair names for indices {}, {}, {}, {}. Cannot trade.",
                    pair1_idx,
                    pair2_idx,
                    pair1_stable_idx,
                    pair2_stable_idx
                );
                return;
            }
        };

    log::debug!(
        "Opportunity found starting with pair {}. ROI: {}",
        pair1_name,
        roi
    );
    let (volume, volume_limited_by_balance) = limiting_volume(pair1, pair2, balance, fee_spot);
    let pair1_amount_in = volume * pair1.ask_price * (1.0 + fee_spot);
    let volume_stable =
        compute_volume_stable(volume, pair2, pair2_stable, fee_spot, fee_stablecoin);

    // Log opportunity even if we can't trade
    log_arbitrage_opportunity(
        pair1_name,
        pair2_name,
        pair1.bid_price,
        pair1.ask_price,
        pair2.bid_price,
        pair2.ask_price,
        pair1.bid_volume,
        pair1.ask_volume,
        pair2.bid_volume,
        pair2.ask_volume,
        pair1_stable.bid_price,
        pair1_stable.ask_price,
        pair2_stable.bid_price,
        pair2_stable.ask_price,
        pair1_stable.bid_volume,
        pair1_stable.ask_volume,
        pair2_stable.bid_volume,
        pair2_stable.ask_volume,
        roi,
        volume,
        pair1_amount_in,
        volume_limited_by_balance,
    );

    if !check_guardrails(volume, pair1, pair2) {
        log::debug!("Not enough volume to trade. Cannot trade.");
        return;
    }

    let send_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    trigger_trades(
        &OrderInfo {
            pair1_name,
            pair2_name,
            pair1_stable_name,
            pair2_stable_name,
            volume_coin: volume,
            volume_stable,
            volume_decimals_coin: pair1.volume_decimals,
            volume_decimals_stable: pair1_stable.volume_decimals,
            send_timestamp,
            pair1_price: pair1.ask_price,
            price_decimals: pair1.price_decimals,
        },
        trade_tx,
    );
}

/// Compute the ROI of an arbitrage opportunity
/// Instead of computing arb_fee each time, compute it once and pass it in
fn compute_roi(
    pair1: &PairData,
    pair2: &PairData,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    arb_fee: f64,
) -> f64 {
    let arb_prices =
        (pair2.bid_price * pair1_stable.bid_price) / (pair1.ask_price * pair2_stable.ask_price);
    arb_prices * arb_fee
}

/// Find the largest amount of volume of COIN we can trade to get the best price levels
/// Also returns whether the volume is limited by the balance, which is useful for logging
/// Assumes the stablecoin has infinite liquidity with no slippage (fair for our purposes)
fn limiting_volume(pair1: &PairData, pair2: &PairData, balance: f64, fee_spot: f64) -> (f64, bool) {
    // Compute the effective volume we can spend based on balance and ask price (with fee)
    let volume_balance = balance / (pair1.ask_price * (1.0 + fee_spot));

    // Take the minimum of effective balance, max ask size, and max bid size
    let min_volume = volume_balance.min(pair1.ask_volume).min(pair2.bid_volume);

    log::debug!(
        "Limiting volume: {}, Balance volume: {}, Pair1 ask volume: {}, Pair2 bid volume: {}",
        min_volume,
        volume_balance,
        pair1.ask_volume,
        pair2.bid_volume
    );

    (min_volume, min_volume == volume_balance)
}

/// Compute the volume of the stablecoin that we can trade
/// Based on how much we make from selling for pair2, then how much we can afford to buy for pair2_stable
/// Small factor of safety to account for minor slippage
fn compute_volume_stable(
    volume: f64,
    pair2: &PairData,
    pair2_stable: &PairData,
    fee_spot: f64,
    fee_stablecoin: f64,
) -> f64 {
    let pair2_amount = volume * pair2.bid_price * (1.0 - fee_spot);
    let volume_stable = pair2_amount / (pair2_stable.ask_price * (1.0 + fee_stablecoin));
    volume_stable * 0.95
}

/// Check if the volume is greater than the minimum order size and minimum cost
fn check_guardrails(volume: f64, pair1: &PairData, pair2: &PairData) -> bool {
    const FACTOR_OF_SAFETY: f64 = 1.01; // Account for minor slippage and fees

    // Check if the volume is greater than the minimum order size
    if volume < pair1.order_min || volume < pair2.order_min {
        return false;
    }

    // Check if the volume is greater than the minimum cost
    if volume < pair1.cost_min * pair1.ask_price * FACTOR_OF_SAFETY
        || volume < pair2.cost_min * pair2.bid_price * FACTOR_OF_SAFETY
    {
        return false;
    }
    true
}

/// Send the signal to start the arbitrage trades.
/// Drops the message immediately if trader is busy (no queuing of stale orders).
fn trigger_trades(order_info: &OrderInfo, trade_tx: mpsc::Sender<OrderInfo>) {
    // Check if trader is busy first - if so, drop immediately
    if TRADER_BUSY.load(Ordering::Relaxed) {
        log::info!("Trader busy, dropping order for {}", order_info.pair1_name);
        return;
    }

    // Try to send the order info to the trading thread
    match trade_tx.try_send(order_info.clone()) {
        Ok(()) => {
            // Successfully sent
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Channel buffer full (shouldn't happen if trader is idle, but handle gracefully)
            log::warn!(
                "Channel buffer full, dropping order for {}",
                order_info.pair1_name
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            log::error!("Trading channel closed, cannot send order");
        }
    }
}
