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
    let fee_spot = fee_spot / 10_000.0;
    let fee_stablecoin = fee_stablecoin / 10_000.0;

    // Calculate arbitrage opportunity
    let arb_fee =
        ((1.0 - fee_spot) * (1.0 - fee_stablecoin)) / ((1.0 + fee_spot) * (1.0 + fee_stablecoin));

    let arb_roi_usd_start = compute_roi(
        usd_pair,
        eur_pair,
        usd_stable_pair,
        eur_stable_pair,
        arb_fee,
    );
    if arb_roi_usd_start > 1.0 {
        log::info!("Arbitrage opportunity found w/ROI {}", arb_roi_usd_start);
        let balance = USD_BALANCE.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let volume = limiting_volume(usd_pair, eur_pair, balance, fee_spot);
        let volume_stable =
            compute_volume_stable(volume, eur_pair, eur_stable_pair, fee_spot, fee_stablecoin);
        if check_guardrails(volume, usd_pair, eur_pair) {
            log::info!("Volume is valid: Trade!");
            let pair1_name = pair_names.get(usd_pair_idx).copied();
            let pair2_name = pair_names.get(eur_pair_idx).copied();
            let pair1_stable_name = pair_names.get(0).copied();
            let pair2_stable_name = pair_names.get(1).copied();
            let volume_decimals_coin = usd_pair.volume_decimals;
            let volume_decimals_stable = usd_stable_pair.volume_decimals;
            let volume_coin = volume;
            if let (
                Some(pair1_name),
                Some(pair2_name),
                Some(pair1_stable_name),
                Some(pair2_stable_name),
            ) = (pair1_name, pair2_name, pair1_stable_name, pair2_stable_name)
            {
                trigger_trades(
                    &OrderInfo {
                        pair1_name,
                        pair2_name,
                        pair1_stable_name,
                        pair2_stable_name,
                        volume_coin,
                        volume_stable,
                        volume_decimals_coin,
                        volume_decimals_stable,
                    },
                    trade_tx.clone(),
                );
            } else {
                log::error!("Failed to get pair names. Cannot trade.");
                return;
            }
        } else {
            log::debug!("Not enough volume to trade. Cannot trade.");
            return;
        }
    }

    let arb_roi_eur_start = compute_roi(
        eur_pair,
        usd_pair,
        eur_stable_pair,
        usd_stable_pair,
        arb_fee,
    );
    if arb_roi_eur_start > 1.0 {
        log::info!("Arbitrage opportunity found w/ROI {}", arb_roi_eur_start);
        let balance = EUR_BALANCE.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let volume = limiting_volume(eur_pair, usd_pair, balance, fee_spot);
        let volume_stable =
            compute_volume_stable(volume, usd_pair, usd_stable_pair, fee_spot, fee_stablecoin);
        if check_guardrails(volume, eur_pair, usd_pair) {
            log::info!("Volume is valid: Trade!");
            let pair1_name = pair_names.get(eur_pair_idx).copied();
            let pair2_name = pair_names.get(usd_pair_idx).copied();
            let pair1_stable_name = pair_names.get(1).copied();
            let pair2_stable_name = pair_names.get(0).copied();
            let volume_decimals_coin = eur_pair.volume_decimals;
            let volume_decimals_stable = eur_stable_pair.volume_decimals;
            let volume_coin = volume;
            if let (
                Some(pair1_name),
                Some(pair2_name),
                Some(pair1_stable_name),
                Some(pair2_stable_name),
            ) = (pair1_name, pair2_name, pair1_stable_name, pair2_stable_name)
            {
                trigger_trades(
                    &OrderInfo {
                        pair1_name,
                        pair2_name,
                        pair1_stable_name,
                        pair2_stable_name,
                        volume_coin,
                        volume_stable,
                        volume_decimals_coin,
                        volume_decimals_stable,
                    },
                    trade_tx.clone(),
                );
            } else {
                log::error!("Failed to get pair names. Cannot trade.");
                return;
            }
        } else {
            log::debug!("Not enough volume to trade. Cannot trade.");
            return;
        }
    }
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
    return arb_prices * arb_fee;
}

/// Find the largest amount of volume of COIN we can trade to get the best price levels
/// Assumes the stablecoin has infinite liquidity with no slippage (fair for our purposes)
fn limiting_volume(pair1: &PairData, pair2: &PairData, balance: f64, fee_spot: f64) -> f64 {
    // Compute the effective volume we can spend based on balance and ask price (with fee)
    let volume_balance = balance / (pair1.ask_price * (1.0 + fee_spot));

    // Take the minimum of effective balance, max ask size, and max bid size
    let min_volume = volume_balance.min(pair1.ask_volume).min(pair2.bid_volume);

    return min_volume;
}

/// Compute the volume of the stablecoin that we can trade
/// Based on how much we make from selling for pair2, then how much we can afford to buy for pair2_stable
/// Small factor of safety to account for minor slippage
fn compute_volume_stable(
    volume: f64,
    pair2: &PairData,
    pair2_stable: &PairData,
    fee_spot: f64,
    fee_stable: f64,
) -> f64 {
    let pair2_amount = volume * pair2.bid_price * (1.0 - fee_spot);
    let volume_stable = pair2_amount / (pair2_stable.ask_price * (1.0 + fee_stable));
    return volume_stable * 0.95;
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
    return true;
}

/// Send the signal to start the arbitrage trades.
/// Only need to specify the first buy order: Other buy orders are implied by the first buy order.
/// Drops the message immediately if trader is busy (no queuing of stale orders).
fn trigger_trades(buy_order: &BuyOrder, trade_tx: mpsc::Sender<BuyOrder>) {
    // Check if trader is busy first - if so, drop immediately
    if TRADER_BUSY.load(Ordering::Relaxed) {
        log::debug!(
            "Trader busy, dropping buy order for {}",
            buy_order.pair_name
        );
        return;
    }

    // Try to send the buy order to the trading thread
    match trade_tx.try_send(buy_order.clone()) {
        Ok(()) => {
            // Successfully sent
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Channel buffer full (shouldn't happen if trader is idle, but handle gracefully)
            log::debug!(
                "Channel buffer full, dropping buy order for {}",
                buy_order.pair_name
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            log::error!("Trading channel closed, cannot send buy order");
        }
    }
}
