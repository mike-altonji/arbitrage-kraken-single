use crate::influx::log_arbitrage_opportunity;
use crate::orderbook::{BboChange, OrderBook, OrderBookVec};
use crate::structs::{OrderInfo, PairData, PairDataVec};
use crate::{EUR_BALANCE, FEE_SPOT, FEE_STABLECOIN, TRADER_BUSY, USD_BALANCE};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WalkMode {
    FixedAskWalkBids,
    FixedBidWalkAsks,
    Skip,
}

impl WalkMode {
    fn as_str(self) -> &'static str {
        match self {
            WalkMode::FixedAskWalkBids => "fixed_ask_walk_bids",
            WalkMode::FixedBidWalkAsks => "fixed_bid_walk_asks",
            WalkMode::Skip => "skip",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct DepthFill {
    volume: f64,
    vwap_ask: f64,
    vwap_bid: f64,
    limit_buy_price: f64,
    blended_roi: f64,
    balance_limited: bool,
    walk_mode: WalkMode,
}

pub fn evaluate_arbitrage(
    pair_data_vec: &PairDataVec,
    order_book_vec: &OrderBookVec,
    idx: usize,
    bbo_change: BboChange,
    pair_names: &[&'static str],
    trade_tx: mpsc::Sender<OrderInfo>,
) {
    let usd_pair_idx = idx - (idx % 2);
    let eur_pair_idx = idx + 1 - (idx % 2);
    let usd_pair = pair_data_vec.get(usd_pair_idx);
    let eur_pair = pair_data_vec.get(eur_pair_idx);
    let usd_stable_pair = pair_data_vec.first();
    let eur_stable_pair = pair_data_vec.get(1);
    let (usd_pair, eur_pair, usd_stable_pair, eur_stable_pair) =
        match (usd_pair, eur_pair, usd_stable_pair, eur_stable_pair) {
            (Some(usd), Some(eur), Some(usd_s), Some(eur_s)) => (usd, eur, usd_s, eur_s),
            _ => {
                log::error!("Failed to get pair for index {} or stablecoin", idx);
                return;
            }
        };

    let usd_book = order_book_vec.get(usd_pair_idx);
    let eur_book = order_book_vec.get(eur_pair_idx);
    let (usd_book, eur_book) = match (usd_book, eur_book) {
        (Some(u), Some(e)) if u.ready && e.ready => (u, e),
        _ => return,
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
    let fee_spot = FEE_SPOT.load(std::sync::atomic::Ordering::Relaxed) as f64 / 10_000.0;
    let fee_stablecoin =
        FEE_STABLECOIN.load(std::sync::atomic::Ordering::Relaxed) as f64 / 10_000.0;
    let arb_fee = (1.0 - fee_spot) / (1.0 + fee_spot);

    let usd_walk = resolve_walk_mode(usd_pair_idx, eur_pair_idx, idx, bbo_change);
    if usd_walk != WalkMode::Skip {
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
                usd_book,
                eur_book,
                usd_stable_pair,
                eur_stable_pair,
                USD_BALANCE.load(std::sync::atomic::Ordering::Relaxed) as f64,
                usd_pair_idx,
                eur_pair_idx,
                0,
                1,
                fee_spot,
                fee_stablecoin,
                arb_fee,
                usd_walk,
                pair_names,
                trade_tx.clone(),
                idx,
            );
        }
    }

    let eur_walk = resolve_walk_mode(eur_pair_idx, usd_pair_idx, idx, bbo_change);
    if eur_walk != WalkMode::Skip {
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
                eur_book,
                usd_book,
                eur_stable_pair,
                usd_stable_pair,
                EUR_BALANCE.load(std::sync::atomic::Ordering::Relaxed) as f64,
                eur_pair_idx,
                usd_pair_idx,
                1,
                0,
                fee_spot,
                fee_stablecoin,
                arb_fee,
                eur_walk,
                pair_names,
                trade_tx.clone(),
                idx,
            );
        }
    }
}

fn resolve_walk_mode(
    pair1_idx: usize,
    pair2_idx: usize,
    updated_idx: usize,
    change: BboChange,
) -> WalkMode {
    if updated_idx == pair1_idx && change.ask_changed() {
        WalkMode::FixedAskWalkBids
    } else if updated_idx == pair2_idx && change.bid_changed() {
        WalkMode::FixedBidWalkAsks
    } else {
        WalkMode::Skip
    }
}

fn roi_at_prices(
    ask_price: f64,
    bid_price: f64,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    arb_fee: f64,
) -> f64 {
    let arb_prices =
        (bid_price * pair1_stable.bid_price) / (ask_price * pair2_stable.ask_price);
    arb_prices * arb_fee
}

#[allow(clippy::too_many_arguments)]
fn compute_depth_fill(
    walk_mode: WalkMode,
    book1: &OrderBook,
    book2: &OrderBook,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    balance: f64,
    fee_spot: f64,
    arb_fee: f64,
) -> Option<DepthFill> {
    match walk_mode {
        WalkMode::FixedAskWalkBids => {
            fixed_ask_walk_bids(book1, book2, pair1_stable, pair2_stable, balance, fee_spot, arb_fee)
        }
        WalkMode::FixedBidWalkAsks => {
            fixed_bid_walk_asks(book1, book2, pair1_stable, pair2_stable, balance, fee_spot, arb_fee)
        }
        WalkMode::Skip => None,
    }
}

/// Fix buy-pair top ask; walk sell-pair bids.
fn fixed_ask_walk_bids(
    book1: &OrderBook,
    book2: &OrderBook,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    balance: f64,
    fee_spot: f64,
    arb_fee: f64,
) -> Option<DepthFill> {
    if book1.ask_count == 0 || book2.bid_count == 0 {
        return None;
    }

    let fixed_ask = &book1.asks[0];
    if fixed_ask.price <= 0.0 || fixed_ask.volume <= 0.0 {
        return None;
    }

    let max_from_balance = balance / (fixed_ask.price * (1.0 + fee_spot));
    let mut remaining_buy = fixed_ask.volume.min(max_from_balance);
    let balance_limited = max_from_balance < fixed_ask.volume;

    let mut bid_i = 0usize;
    let mut bid_rem = book2.bids[0].volume;
    let mut total_vol = 0.0;
    let mut total_proceeds = 0.0;

    while bid_i < book2.bid_count as usize && remaining_buy > 0.0 {
        let bid = &book2.bids[bid_i];
        if bid.price <= 0.0 || bid.volume <= 0.0 {
            break;
        }

        let roi = roi_at_prices(
            fixed_ask.price,
            bid.price,
            pair1_stable,
            pair2_stable,
            arb_fee,
        );
        if roi <= 1.0 {
            break;
        }

        let trade = remaining_buy.min(bid_rem);
        if trade <= 0.0 {
            break;
        }

        total_vol += trade;
        total_proceeds += trade * bid.price;
        remaining_buy -= trade;
        bid_rem -= trade;

        if bid_rem <= 0.0 {
            bid_i += 1;
            if bid_i < book2.bid_count as usize {
                bid_rem = book2.bids[bid_i].volume;
            }
        }
    }

    if total_vol <= 0.0 {
        return None;
    }

    let blended_roi = roi_at_prices(
        fixed_ask.price,
        total_proceeds / total_vol,
        pair1_stable,
        pair2_stable,
        arb_fee,
    );

    Some(DepthFill {
        volume: total_vol,
        vwap_ask: fixed_ask.price,
        vwap_bid: total_proceeds / total_vol,
        limit_buy_price: fixed_ask.price,
        blended_roi,
        balance_limited,
        walk_mode: WalkMode::FixedAskWalkBids,
    })
}

/// Fix sell-pair top bid; walk buy-pair asks.
fn fixed_bid_walk_asks(
    book1: &OrderBook,
    book2: &OrderBook,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    balance: f64,
    fee_spot: f64,
    arb_fee: f64,
) -> Option<DepthFill> {
    if book1.ask_count == 0 || book2.bid_count == 0 {
        return None;
    }

    let fixed_bid = &book2.bids[0];
    if fixed_bid.price <= 0.0 || fixed_bid.volume <= 0.0 {
        return None;
    }

    let mut remaining_sell = fixed_bid.volume;
    let mut balance_remaining = balance;
    let mut balance_limited = false;

    let mut ask_i = 0usize;
    let mut ask_rem = book1.asks[0].volume;
    let mut total_vol = 0.0;
    let mut total_cost = 0.0;
    let mut limit_buy_price = 0.0;

    while ask_i < book1.ask_count as usize && remaining_sell > 0.0 && balance_remaining > 0.0 {
        let ask = &book1.asks[ask_i];
        if ask.price <= 0.0 || ask.volume <= 0.0 {
            break;
        }

        let roi = roi_at_prices(
            ask.price,
            fixed_bid.price,
            pair1_stable,
            pair2_stable,
            arb_fee,
        );
        if roi <= 1.0 {
            break;
        }

        let max_from_balance = balance_remaining / (ask.price * (1.0 + fee_spot));
        let desired = remaining_sell.min(ask_rem);
        let trade = desired.min(max_from_balance);
        if trade <= 0.0 {
            break;
        }
        if trade < desired {
            balance_limited = true;
        }

        total_vol += trade;
        total_cost += trade * ask.price;
        limit_buy_price = ask.price;
        remaining_sell -= trade;
        ask_rem -= trade;
        balance_remaining -= trade * ask.price * (1.0 + fee_spot);

        if ask_rem <= 0.0 {
            ask_i += 1;
            if ask_i < book1.ask_count as usize {
                ask_rem = book1.asks[ask_i].volume;
            }
        }
    }

    if total_vol <= 0.0 {
        return None;
    }

    let vwap_ask = total_cost / total_vol;
    let blended_roi = roi_at_prices(
        vwap_ask,
        fixed_bid.price,
        pair1_stable,
        pair2_stable,
        arb_fee,
    );

    Some(DepthFill {
        volume: total_vol,
        vwap_ask,
        vwap_bid: fixed_bid.price,
        limit_buy_price,
        blended_roi,
        balance_limited,
        walk_mode: WalkMode::FixedBidWalkAsks,
    })
}

/// Process an arbitrage opportunity: check volumes, guardrails, and trigger trades
#[allow(clippy::too_many_arguments)]
fn process_arbitrage_opportunity(
    roi: f64,
    pair1: &PairData,
    pair2: &PairData,
    book1: &OrderBook,
    book2: &OrderBook,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    balance: f64,
    pair1_idx: usize,
    pair2_idx: usize,
    pair1_stable_idx: usize,
    pair2_stable_idx: usize,
    fee_spot: f64,
    fee_stablecoin: f64,
    arb_fee: f64,
    walk_mode: WalkMode,
    pair_names: &[&'static str],
    trade_tx: mpsc::Sender<OrderInfo>,
    updated_pair_idx: usize,
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

    let depth = match compute_depth_fill(
        walk_mode,
        book1,
        book2,
        pair1_stable,
        pair2_stable,
        balance,
        fee_spot,
        arb_fee,
    ) {
        Some(d) => d,
        None => {
            log::debug!(
                "No profitable depth volume for {} (walk={})",
                pair1_name,
                walk_mode.as_str()
            );
            return;
        }
    };

    log::debug!(
        "Opportunity found starting with pair {}. BBO ROI: {}, blended ROI: {}, vol: {}, walk: {}",
        pair1_name,
        roi,
        depth.blended_roi,
        depth.volume,
        walk_mode.as_str()
    );

    let pair1_amount_in = depth.volume * depth.vwap_ask * (1.0 + fee_spot);
    let volume_stable = compute_volume_stable(
        depth.volume,
        depth.vwap_bid,
        pair2_stable,
        fee_spot,
        fee_stablecoin,
    );

    // Get the updated pair's kraken_ts for guardrails
    let updated_pair_kraken_ts = if updated_pair_idx == pair1_idx {
        pair1.kraken_ts
    } else if updated_pair_idx == pair2_idx {
        pair2.kraken_ts
    } else {
        log::error!("Updated pair idx doesn't match pair1 or pair2 idx. Using pair1 kraken ts.");
        pair1.kraken_ts
    };

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
        depth.volume,
        pair1_amount_in,
        depth.balance_limited,
        walk_mode.as_str(),
        depth.vwap_ask,
        depth.vwap_bid,
        depth.blended_roi,
        depth.limit_buy_price,
    );

    if !check_guardrails(depth.volume, depth.vwap_ask, depth.vwap_bid, pair1, pair2) {
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
            volume_coin: depth.volume,
            volume_stable,
            volume_decimals_coin: pair1.volume_decimals,
            volume_decimals_stable: pair1_stable.volume_decimals,
            send_timestamp,
            pair1_price: depth.limit_buy_price,
            price_decimals: pair1.price_decimals,
            updated_pair_kraken_ts,
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
    roi_at_prices(
        pair1.ask_price,
        pair2.bid_price,
        pair1_stable,
        pair2_stable,
        arb_fee,
    )
}

/// Compute the volume of the stablecoin that we can trade
/// Based on how much we make from selling for pair2, then how much we can afford to buy for pair2_stable
/// Small factor of safety to account for minor slippage
fn compute_volume_stable(
    volume: f64,
    vwap_bid: f64,
    pair2_stable: &PairData,
    fee_spot: f64,
    fee_stablecoin: f64,
) -> f64 {
    let pair2_amount = volume * vwap_bid * (1.0 - fee_spot);
    let volume_stable = pair2_amount / (pair2_stable.ask_price * (1.0 + fee_stablecoin));
    volume_stable * 0.95
}

/// Check if the volume is greater than the minimum order size and minimum cost
fn check_guardrails(
    volume: f64,
    vwap_ask: f64,
    vwap_bid: f64,
    pair1: &PairData,
    pair2: &PairData,
) -> bool {
    const FACTOR_OF_SAFETY: f64 = 1.01;

    // Check if the volume is greater than the minimum order size
    if volume < pair1.order_min || volume < pair2.order_min {
        return false;
    }

    // Check if the volume is greater than the minimum cost
    if volume < pair1.cost_min * vwap_ask * FACTOR_OF_SAFETY
        || volume < pair2.cost_min * vwap_bid * FACTOR_OF_SAFETY
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::OrderBook;

    fn stable_pair(bid: f64, ask: f64) -> PairData {
        PairData {
            bid_price: bid,
            ask_price: ask,
            bid_volume: 1_000_000.0,
            ask_volume: 1_000_000.0,
            order_min: 0.0,
            cost_min: 0.0,
            price_decimals: 4,
            volume_decimals: 8,
            pair_status: true,
            kraken_ts: 0.0,
        }
    }

    fn coin_pair() -> PairData {
        PairData {
            bid_price: 100.0,
            ask_price: 101.0,
            bid_volume: 1.0,
            ask_volume: 1.0,
            order_min: 0.001,
            cost_min: 0.0,
            price_decimals: 2,
            volume_decimals: 8,
            pair_status: true,
            kraken_ts: 0.0,
        }
    }

    fn book_from_levels(
        asks: &[(&str, &str)],
        bids: &[(&str, &str)],
    ) -> OrderBook {
        let ask_levels: Vec<(&str, &str, f64)> = asks
            .iter()
            .map(|&(p, v)| (p, v, 1.0))
            .collect();
        let bid_levels: Vec<(&str, &str, f64)> = bids
            .iter()
            .map(|&(p, v)| (p, v, 1.0))
            .collect();
        let mut book = OrderBook::new();
        book.apply_snapshot(&ask_levels, &bid_levels);
        book
    }

    const ARB_FEE: f64 = 0.992;
    const FEE_SPOT: f64 = 0.004;

    #[test]
    fn resolve_walk_mode_usd_ask_triggers_fixed_ask() {
        let change = BboChange {
            changed: true,
            ask_price_changed: true,
            ..BboChange::default()
        };
        assert_eq!(
            resolve_walk_mode(2, 3, 2, change),
            WalkMode::FixedAskWalkBids
        );
    }

    #[test]
    fn resolve_walk_mode_irrelevant_bid_skips_usd_to_eur() {
        let change = BboChange {
            changed: true,
            bid_price_changed: true,
            ..BboChange::default()
        };
        assert_eq!(resolve_walk_mode(2, 3, 2, change), WalkMode::Skip);
    }

    #[test]
    fn resolve_walk_mode_eur_bid_triggers_fixed_bid() {
        let change = BboChange {
            changed: true,
            bid_price_changed: true,
            ..BboChange::default()
        };
        assert_eq!(
            resolve_walk_mode(2, 3, 3, change),
            WalkMode::FixedBidWalkAsks
        );
    }

    #[test]
    fn fixed_ask_walk_bids_uses_deep_bids() {
        let book1 = book_from_levels(&[("100.0", "10.0")], &[("99.0", "1.0")]);
        let book2 = book_from_levels(
            &[("200.0", "1.0")],
            &[("105.0", "1.0"), ("104.0", "5.0")],
        );
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let fill = fixed_ask_walk_bids(&book1, &book2, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE)
            .expect("fill");

        assert_eq!(fill.walk_mode, WalkMode::FixedAskWalkBids);
        assert!((fill.volume - 6.0).abs() < 1e-9);
        assert!((fill.limit_buy_price - 100.0).abs() < 1e-9);
        assert!((fill.vwap_bid - (105.0 + 5.0 * 104.0) / 6.0).abs() < 1e-9);
    }

    #[test]
    fn fixed_ask_walk_bids_stops_at_unprofitable_bid() {
        let book1 = book_from_levels(&[("100.0", "10.0")], &[]);
        let book2 = book_from_levels(
            &[],
            &[("105.0", "2.0"), ("100.5", "5.0")],
        );
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let fill = fixed_ask_walk_bids(&book1, &book2, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE)
            .expect("fill");

        assert!((fill.volume - 2.0).abs() < 1e-9);
    }

    #[test]
    fn fixed_bid_walk_asks_uses_deep_asks() {
        let book1 = book_from_levels(
            &[("100.0", "1.0"), ("101.0", "5.0")],
            &[],
        );
        let book2 = book_from_levels(&[], &[("105.0", "10.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let fill = fixed_bid_walk_asks(&book1, &book2, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE)
            .expect("fill");

        assert_eq!(fill.walk_mode, WalkMode::FixedBidWalkAsks);
        assert!((fill.volume - 6.0).abs() < 1e-9);
        assert!((fill.limit_buy_price - 101.0).abs() < 1e-9);
    }

    #[test]
    fn fixed_ask_walk_bids_balance_limited() {
        let book1 = book_from_levels(&[("100.0", "10.0")], &[]);
        let book2 = book_from_levels(&[], &[("105.0", "10.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let balance = 150.0;
        let fill = fixed_ask_walk_bids(&book1, &book2, &s1, &s2, balance, FEE_SPOT, ARB_FEE)
            .expect("fill");

        let max_vol = balance / (100.0 * (1.0 + FEE_SPOT));
        assert!((fill.volume - max_vol).abs() < 1e-9);
        assert!(fill.balance_limited);
    }

    #[test]
    fn same_pair_ask_and_bid_change_resolve_different_directions() {
        let change = BboChange {
            changed: true,
            ask_price_changed: true,
            bid_price_changed: true,
            ..BboChange::default()
        };
        assert_eq!(
            resolve_walk_mode(2, 3, 2, change),
            WalkMode::FixedAskWalkBids
        );
        assert_eq!(
            resolve_walk_mode(3, 2, 2, change),
            WalkMode::FixedBidWalkAsks
        );
    }

    #[test]
    fn check_guardrails_uses_vwap() {
        let pair1 = coin_pair();
        let pair2 = coin_pair();
        assert!(check_guardrails(1.0, 101.0, 100.0, &pair1, &pair2));
        assert!(!check_guardrails(0.0001, 101.0, 100.0, &pair1, &pair2));
    }
}
