use crate::arb_forensics::{
    next_opportunity_id, try_log, ArbOpportunityEvent, ForensicsEvent, MakerEvent, PlannedSlice,
};
use crate::influx::log_arbitrage_opportunity;
use crate::maker::{build_desired, publish_desired, DesiredQuote};
use crate::momentum::maybe_momentum_order;
use crate::orderbook::{BboChange, OrderBook, OrderBookVec, BOOK_DEPTH};
use crate::structs::{OrderInfo, PairData, PairDataVec, TradeCommand};
use crate::{
    DEPTH_HAIRCUT_PCT, EUR_BALANCE, FEE_SPOT, FEE_STABLECOIN, MAKER_ENABLED, MAX_WALK_DEPTH,
    MOMENTUM_ENABLED, ROI_BUFFER_BPS, TRADER_BUSY, USD_BALANCE,
};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// Up to BOOK_DEPTH walked levels on each leg.
const SLICE_CAP: usize = 2 * BOOK_DEPTH;

/// Which BBO improvement triggered this evaluation. Telemetry replacement for
/// the old walk_mode tag: `bid_improved` means the updated pair is the sell
/// leg; `ask_improved` means it is the buy leg.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Trigger {
    BidImproved,
    AskImproved,
}

impl Trigger {
    fn as_str(self) -> &'static str {
        match self {
            Trigger::BidImproved => "bid_improved",
            Trigger::AskImproved => "ask_improved",
        }
    }
}

/// Depth-walk risk knobs, loaded from the CLI-configurable statics.
#[derive(Clone, Copy, Debug)]
struct WalkKnobs {
    /// Max levels consumed per side.
    max_depth: usize,
    /// Fraction (0..=1] of displayed volume planned at levels beyond L0.
    haircut: f64,
    /// Marginal ROI must exceed this to keep walking (1 + buffer).
    roi_floor: f64,
}

fn load_walk_knobs() -> WalkKnobs {
    let max_depth = MAX_WALK_DEPTH.load(Ordering::Relaxed).clamp(1, BOOK_DEPTH as i16) as usize;
    let haircut = (DEPTH_HAIRCUT_PCT.load(Ordering::Relaxed).clamp(1, 100) as f64) / 100.0;
    let roi_floor = 1.0 + (ROI_BUFFER_BPS.load(Ordering::Relaxed).max(0) as f64) / 10_000.0;
    WalkKnobs {
        max_depth,
        haircut,
        roi_floor,
    }
}

#[derive(Clone, Debug, PartialEq)]
struct DepthFill {
    volume: f64,
    vwap_ask: f64,
    vwap_bid: f64,
    limit_buy_price: f64,
    blended_roi: f64,
    balance_limited: bool,
    slices: [PlannedSlice; SLICE_CAP],
    slice_count: u8,
    stop_reason: &'static str,
    expected_cost: f64,
    expected_proceeds: f64,
    expected_pnl: f64,
}

impl DepthFill {
    fn slices_vec(&self) -> Vec<PlannedSlice> {
        self.slices[..self.slice_count as usize].to_vec()
    }

    fn expected_pnl_bps(&self) -> f64 {
        if self.expected_cost > 0.0 {
            self.expected_pnl / self.expected_cost * 10_000.0
        } else {
            0.0
        }
    }
}

/// Sibling crypto pair sharing the same coin (USD pairs are even, EUR odd).
fn sibling_pair_idx(idx: usize) -> usize {
    if idx.is_multiple_of(2) {
        idx + 1
    } else {
        idx - 1
    }
}

pub fn evaluate_arbitrage(
    pair_data_vec: &PairDataVec,
    order_book_vec: &OrderBookVec,
    idx: usize,
    bbo_change: BboChange,
    pair_names: &[&'static str],
    trade_tx: mpsc::Sender<TradeCommand>,
) {
    // Arb/momentum only react to improvements; maker must react to ANY BBO
    // change (a worsening bid still moves the fair value we quote around),
    // so this gate is applied after the maker branch below.
    let maker_on = MAKER_ENABLED.load(Ordering::Relaxed);
    if !maker_on && !bbo_change.bid_improved && !bbo_change.ask_improved {
        return;
    }

    let x_idx = idx;
    let y_idx = sibling_pair_idx(idx);

    let x_pair = pair_data_vec.get(x_idx);
    let y_pair = pair_data_vec.get(y_idx);
    let usd_stable_pair = pair_data_vec.first();
    let eur_stable_pair = pair_data_vec.get(1);
    let (x_pair, y_pair, usd_stable_pair, eur_stable_pair) =
        match (x_pair, y_pair, usd_stable_pair, eur_stable_pair) {
            (Some(x), Some(y), Some(us), Some(es)) => (x, y, us, es),
            _ => {
                log::error!("Failed to get pair for index {} or stablecoin", idx);
                return;
            }
        };

    // Maker mode takes exclusive control: skip arb and momentum entirely.
    // Runs before the readiness/price gates below so that bad data publishes
    // cancel-desires instead of leaving quotes resting. A tick on either pair
    // of the couple reprices BOTH pairs — each one's fair value comes from
    // the other, so the sibling's move is exactly when our quote goes stale.
    if maker_on {
        let x_name = pair_names.get(x_idx).copied().unwrap_or("");
        let y_name = pair_names.get(y_idx).copied().unwrap_or("");
        if x_name.is_empty() || y_name.is_empty() {
            return;
        }
        let stable_idx = |pair_idx: usize| -> &PairData {
            if pair_idx.is_multiple_of(2) {
                usd_stable_pair
            } else {
                eur_stable_pair
            }
        };
        maker_publish_pair(
            x_pair,
            y_pair,
            stable_idx(x_idx),
            stable_idx(y_idx),
            x_name,
            y_name,
            order_book_vec.get(x_idx),
        );
        maker_publish_pair(
            y_pair,
            x_pair,
            stable_idx(y_idx),
            stable_idx(x_idx),
            y_name,
            x_name,
            order_book_vec.get(y_idx),
        );
        return;
    }

    let x_book = order_book_vec.get(x_idx);
    let y_book = order_book_vec.get(y_idx);
    let (x_book, y_book) = match (x_book, y_book) {
        (Some(x), Some(y)) if x.ready && y.ready => (x, y),
        _ => return,
    };

    if !x_pair.pair_status
        || !y_pair.pair_status
        || !usd_stable_pair.pair_status
        || !eur_stable_pair.pair_status
    {
        return;
    }

    if x_pair.bid_price == 0.0
        || x_pair.ask_price == 0.0
        || y_pair.bid_price == 0.0
        || y_pair.ask_price == 0.0
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

    // Even idx → USD-quoted pair (stable idx 0), odd → EUR-quoted (stable idx 1).
    let stable_for = |pair_idx: usize| -> &PairData {
        if pair_idx.is_multiple_of(2) {
            usd_stable_pair
        } else {
            eur_stable_pair
        }
    };

    let fee_spot = FEE_SPOT.load(Ordering::Relaxed) as f64 / 10_000.0;
    let fee_stablecoin = FEE_STABLECOIN.load(Ordering::Relaxed) as f64 / 10_000.0;
    let arb_fee = (1.0 - fee_spot) / (1.0 + fee_spot);
    let knobs = load_walk_knobs();

    let balance_for = |pair_idx: usize| -> f64 {
        if pair_idx.is_multiple_of(2) {
            USD_BALANCE.load(Ordering::Relaxed) as f64
        } else {
            EUR_BALANCE.load(Ordering::Relaxed) as f64
        }
    };

    // X's bid improved: X is the sell leg. Momentum (when enabled) gets first
    // claim on the event; if its gate produces an order, arb is skipped.
    if bbo_change.bid_improved {
        let mut momentum_consumed = false;
        if MOMENTUM_ENABLED.load(Ordering::Relaxed) {
            let x_name = pair_names.get(x_idx).copied().unwrap_or("");
            let y_name = pair_names.get(y_idx).copied().unwrap_or("");
            if !x_name.is_empty() && !y_name.is_empty() {
                if let Some(order) = maybe_momentum_order(
                    x_pair,
                    y_pair,
                    stable_for(x_idx),
                    stable_for(y_idx),
                    x_name,
                    y_name,
                    balance_for(y_idx),
                    fee_spot,
                ) {
                    send_trade_command(TradeCommand::Momentum(order), y_name, &trade_tx);
                    momentum_consumed = true;
                }
            }
        }

        if !momentum_consumed {
            let roi = compute_roi(y_pair, x_pair, stable_for(y_idx), stable_for(x_idx), arb_fee);
            if roi > knobs.roi_floor {
                process_arbitrage_opportunity(
                    roi,
                    y_pair,
                    x_pair,
                    y_book,
                    x_book,
                    stable_for(y_idx),
                    stable_for(x_idx),
                    balance_for(y_idx),
                    y_idx,
                    x_idx,
                    y_idx % 2,
                    x_idx % 2,
                    fee_spot,
                    fee_stablecoin,
                    arb_fee,
                    knobs,
                    Trigger::BidImproved,
                    pair_names,
                    trade_tx.clone(),
                    idx,
                );
            }
        }
    }

    // X's ask improved: X is the buy leg.
    if bbo_change.ask_improved {
        let roi = compute_roi(x_pair, y_pair, stable_for(x_idx), stable_for(y_idx), arb_fee);
        if roi > knobs.roi_floor {
            process_arbitrage_opportunity(
                roi,
                x_pair,
                y_pair,
                x_book,
                y_book,
                stable_for(x_idx),
                stable_for(y_idx),
                balance_for(x_idx),
                x_idx,
                y_idx,
                x_idx % 2,
                y_idx % 2,
                fee_spot,
                fee_stablecoin,
                arb_fee,
                knobs,
                Trigger::AskImproved,
                pair_names,
                trade_tx.clone(),
                idx,
            );
        }
    }
}

/// Build and publish the desired maker quotes for one pair; log a
/// `maker_desire` forensics event when the desire materially changed.
#[allow(clippy::too_many_arguments)]
fn maker_publish_pair(
    target: &PairData,
    sibling: &PairData,
    target_stable: &PairData,
    sibling_stable: &PairData,
    target_name: &'static str,
    sibling_name: &'static str,
    target_book: Option<&OrderBook>,
) {
    let desire = build_desired(
        target,
        sibling,
        target_stable,
        sibling_stable,
        target_name,
        sibling_name,
        target_book,
    );
    let event = maker_desire_event(&desire);
    if publish_desired(desire) {
        try_log(ForensicsEvent::Maker(event));
    }
}

fn maker_desire_event(d: &DesiredQuote) -> MakerEvent {
    let side = match (d.bid.is_some(), d.ask.is_some()) {
        (true, true) => "both",
        (true, false) => "buy",
        (false, true) => "sell",
        (false, false) => "none",
    };
    MakerEvent {
        event: "maker_desire",
        opportunity_id: d.opportunity_id,
        pair: d.pair_name.to_string(),
        sibling: d.sibling_name,
        side,
        fair_mid: d.fair_mid,
        bid_price: d.bid.map(|q| q.price).unwrap_or(0.0),
        bid_volume: d.bid.map(|q| q.volume).unwrap_or(0.0),
        ask_price: d.ask.map(|q| q.price).unwrap_or(0.0),
        ask_volume: d.ask.map(|q| q.volume).unwrap_or(0.0),
        price: 0.0,
        volume: 0.0,
        userref: 0,
        inventory_coin: d.inventory_coin,
        fee: 0.0,
        realized_pnl: 0.0,
        session_pnl: crate::maker::session_realized_pnl(),
        reason: d.reason.to_string(),
        event_ts_ns: d.eval_ts_ns,
    }
}

/// Reprice every crypto pair on this listener. Called when a stablecoin (FX)
/// pair ticks: the FX legs feed every pair's fair value, so all quotes are
/// potentially stale. The publish path coalesces unchanged desires, so this
/// is cheap when quotes are already correct.
pub fn maker_reprice_all(
    pair_data_vec: &PairDataVec,
    order_book_vec: &OrderBookVec,
    pair_names: &[&'static str],
) {
    let (Some(usd_stable_pair), Some(eur_stable_pair)) =
        (pair_data_vec.first(), pair_data_vec.get(1))
    else {
        return;
    };
    for target_idx in 2..pair_data_vec.len() {
        let sibling_idx = sibling_pair_idx(target_idx);
        let (Some(target), Some(sibling)) =
            (pair_data_vec.get(target_idx), pair_data_vec.get(sibling_idx))
        else {
            continue;
        };
        let (Some(target_name), Some(sibling_name)) = (
            pair_names.get(target_idx).copied(),
            pair_names.get(sibling_idx).copied(),
        ) else {
            continue;
        };
        let stable = |pair_idx: usize| -> &PairData {
            if pair_idx.is_multiple_of(2) {
                usd_stable_pair
            } else {
                eur_stable_pair
            }
        };
        maker_publish_pair(
            target,
            sibling,
            stable(target_idx),
            stable(sibling_idx),
            target_name,
            sibling_name,
            order_book_vec.get(target_idx),
        );
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

/// Convert a pair2-quote amount into pair1-quote using the same stable legs as ROI.
fn quote2_to_quote1(amount_quote2: f64, pair1_stable: &PairData, pair2_stable: &PairData) -> f64 {
    if pair2_stable.ask_price <= 0.0 {
        return 0.0;
    }
    amount_quote2 * pair1_stable.bid_price / pair2_stable.ask_price
}

fn quote2_to_quote1_fx(pair1_stable: &PairData, pair2_stable: &PairData) -> f64 {
    if pair2_stable.ask_price <= 0.0 {
        return 0.0;
    }
    pair1_stable.bid_price / pair2_stable.ask_price
}

fn empty_slices() -> [PlannedSlice; SLICE_CAP] {
    [PlannedSlice {
        leg: "",
        level_idx: 0,
        price: 0.0,
        book_volume: 0.0,
        planned_volume: 0.0,
        roi_at_level: 0.0,
    }; SLICE_CAP]
}

fn push_slice(slices: &mut [PlannedSlice; SLICE_CAP], count: &mut u8, slice: PlannedSlice) {
    if (*count as usize) < SLICE_CAP {
        slices[*count as usize] = slice;
        *count += 1;
    }
}

/// Greedy dual-pointer walk over buy-book asks and sell-book bids.
///
/// Marginal ROI is monotonically non-increasing in both pointers, so matching
/// the current cheapest ask against the current best bid while marginal ROI
/// clears the floor is exactly optimal for expected PnL. Volume at levels
/// beyond L0 is haircut by `knobs.haircut`; each side is capped at
/// `knobs.max_depth` levels. `limit_buy_price` is the deepest ask touched —
/// a single limit IOC at that price sweeps every planned buy level.
#[allow(clippy::too_many_arguments)]
fn dual_walk(
    book_buy: &OrderBook,
    book_sell: &OrderBook,
    pair1_stable: &PairData,
    pair2_stable: &PairData,
    balance: f64,
    fee_spot: f64,
    arb_fee: f64,
    knobs: WalkKnobs,
) -> Option<DepthFill> {
    if book_buy.ask_count == 0 || book_sell.bid_count == 0 {
        return None;
    }

    let ask_depth = (book_buy.ask_count as usize).min(knobs.max_depth);
    let bid_depth = (book_sell.bid_count as usize).min(knobs.max_depth);

    let level_volume = |volume: f64, level: usize| {
        if level == 0 {
            volume
        } else {
            volume * knobs.haircut
        }
    };

    let mut i = 0usize;
    let mut j = 0usize;
    let mut ask_rem = level_volume(book_buy.asks[0].volume, 0);
    let mut bid_rem = level_volume(book_sell.bids[0].volume, 0);
    let mut balance_remaining = balance;
    let mut balance_limited = false;

    let mut total_vol = 0.0;
    let mut total_cost = 0.0;
    let mut total_proceeds = 0.0;
    let mut limit_buy_price = 0.0;

    // Per-level accumulators so each (leg, level) yields one slice.
    let mut ask_acc = 0.0;
    let mut bid_acc = 0.0;
    let mut ask_roi_first = 0.0;
    let mut bid_roi_first = 0.0;

    let mut ask_slices = empty_slices();
    let mut ask_slice_count = 0u8;
    let mut bid_slices = empty_slices();
    let mut bid_slice_count = 0u8;

    let mut stop_reason: &'static str = "";

    while i < ask_depth && j < bid_depth {
        let ask = &book_buy.asks[i];
        let bid = &book_sell.bids[j];
        if ask.price <= 0.0 || ask.volume <= 0.0 || bid.price <= 0.0 || bid.volume <= 0.0 {
            stop_reason = "book_depth_exhausted";
            break;
        }

        let roi = roi_at_prices(ask.price, bid.price, pair1_stable, pair2_stable, arb_fee);
        if roi <= knobs.roi_floor {
            stop_reason = "unprofitable_level";
            break;
        }

        let max_from_balance = balance_remaining / (ask.price * (1.0 + fee_spot));
        let desired = ask_rem.min(bid_rem);
        let trade = desired.min(max_from_balance);
        if trade <= 0.0 {
            if max_from_balance < desired {
                balance_limited = true;
                stop_reason = "balance_capped";
            }
            break;
        }
        if trade < desired {
            balance_limited = true;
            stop_reason = "balance_capped";
        }

        if ask_acc == 0.0 {
            ask_roi_first = roi;
        }
        if bid_acc == 0.0 {
            bid_roi_first = roi;
        }
        ask_acc += trade;
        bid_acc += trade;
        total_vol += trade;
        total_cost += trade * ask.price;
        total_proceeds += trade * bid.price;
        limit_buy_price = ask.price;
        balance_remaining -= trade * ask.price * (1.0 + fee_spot);
        ask_rem -= trade;
        bid_rem -= trade;

        if balance_limited {
            break;
        }

        if ask_rem <= 0.0 {
            push_slice(
                &mut ask_slices,
                &mut ask_slice_count,
                PlannedSlice {
                    leg: "walk_ask",
                    level_idx: i as u8,
                    price: ask.price,
                    book_volume: ask.volume,
                    planned_volume: ask_acc,
                    roi_at_level: ask_roi_first,
                },
            );
            ask_acc = 0.0;
            i += 1;
            if i < ask_depth {
                ask_rem = level_volume(book_buy.asks[i].volume, i);
            }
        }
        if bid_rem <= 0.0 {
            push_slice(
                &mut bid_slices,
                &mut bid_slice_count,
                PlannedSlice {
                    leg: "walk_bid",
                    level_idx: j as u8,
                    price: bid.price,
                    book_volume: bid.volume,
                    planned_volume: bid_acc,
                    roi_at_level: bid_roi_first,
                },
            );
            bid_acc = 0.0;
            j += 1;
            if j < bid_depth {
                bid_rem = level_volume(book_sell.bids[j].volume, j);
            }
        }
    }

    if total_vol <= 0.0 {
        return None;
    }

    // Flush partially consumed levels.
    if ask_acc > 0.0 && i < ask_depth {
        let ask = &book_buy.asks[i];
        push_slice(
            &mut ask_slices,
            &mut ask_slice_count,
            PlannedSlice {
                leg: "walk_ask",
                level_idx: i as u8,
                price: ask.price,
                book_volume: ask.volume,
                planned_volume: ask_acc,
                roi_at_level: ask_roi_first,
            },
        );
    }
    if bid_acc > 0.0 && j < bid_depth {
        let bid = &book_sell.bids[j];
        push_slice(
            &mut bid_slices,
            &mut bid_slice_count,
            PlannedSlice {
                leg: "walk_bid",
                level_idx: j as u8,
                price: bid.price,
                book_volume: bid.volume,
                planned_volume: bid_acc,
                roi_at_level: bid_roi_first,
            },
        );
    }

    if stop_reason.is_empty() {
        let ask_capped = i >= ask_depth && ask_depth < book_buy.ask_count as usize;
        let bid_capped = j >= bid_depth && bid_depth < book_sell.bid_count as usize;
        stop_reason = if ask_capped || bid_capped {
            "depth_cap_reached"
        } else {
            "book_depth_exhausted"
        };
    }

    // Report buy-leg slices first, then sell-leg slices.
    let mut slices = empty_slices();
    let mut slice_count = 0u8;
    for slice in ask_slices.iter().take(ask_slice_count as usize) {
        push_slice(&mut slices, &mut slice_count, *slice);
    }
    for slice in bid_slices.iter().take(bid_slice_count as usize) {
        push_slice(&mut slices, &mut slice_count, *slice);
    }

    let vwap_ask = total_cost / total_vol;
    let vwap_bid = total_proceeds / total_vol;
    let expected_cost = total_cost * (1.0 + fee_spot);
    let expected_proceeds = quote2_to_quote1(
        total_proceeds * (1.0 - fee_spot),
        pair1_stable,
        pair2_stable,
    );
    let expected_pnl = expected_proceeds - expected_cost;
    let blended_roi = roi_at_prices(vwap_ask, vwap_bid, pair1_stable, pair2_stable, arb_fee);

    Some(DepthFill {
        volume: total_vol,
        vwap_ask,
        vwap_bid,
        limit_buy_price,
        blended_roi,
        balance_limited,
        slices,
        slice_count,
        stop_reason,
        expected_cost,
        expected_proceeds,
        expected_pnl,
    })
}

fn guardrail_failure_reason(
    volume: f64,
    vwap_ask: f64,
    vwap_bid: f64,
    pair1: &PairData,
    pair2: &PairData,
) -> Option<&'static str> {
    const FACTOR_OF_SAFETY: f64 = 1.01;
    if volume < pair1.order_min || volume < pair2.order_min {
        return Some("below_min_order");
    }
    if volume < pair1.cost_min * vwap_ask * FACTOR_OF_SAFETY
        || volume < pair2.cost_min * vwap_bid * FACTOR_OF_SAFETY
    {
        return Some("below_min_cost");
    }
    None
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
    knobs: WalkKnobs,
    trigger: Trigger,
    pair_names: &[&'static str],
    trade_tx: mpsc::Sender<TradeCommand>,
    updated_pair_idx: usize,
) {
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

    let opportunity_id = next_opportunity_id();
    let eval_ts_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let depth = match dual_walk(
        book1,
        book2,
        pair1_stable,
        pair2_stable,
        balance,
        fee_spot,
        arb_fee,
        knobs,
    ) {
        Some(d) => d,
        None => {
            log::debug!(
                "No profitable depth volume for {} (trigger={})",
                pair1_name,
                trigger.as_str()
            );
            try_log(ForensicsEvent::ArbOpportunity(ArbOpportunityEvent {
                event: "arb_opportunity",
                opportunity_id,
                trigger: trigger.as_str(),
                pair1: pair1_name,
                pair2: pair2_name,
                bbo_roi: roi,
                blended_roi: 0.0,
                depth_volume: 0.0,
                l1_limiting_volume: pair1.ask_volume.min(pair2.bid_volume),
                depth_multiplier: 0.0,
                vwap_ask: pair1.ask_price,
                vwap_bid: pair2.bid_price,
                limit_buy_price: pair1.ask_price,
                expected_cost: 0.0,
                expected_proceeds: 0.0,
                expected_pnl: 0.0,
                expected_pnl_bps: 0.0,
                stop_reason: "no_depth_volume",
                balance_limited: false,
                decision: "no_depth_volume",
                kraken_ts: if updated_pair_idx == pair1_idx {
                    pair1.kraken_ts
                } else {
                    pair2.kraken_ts
                },
                eval_ts_ns,
                slices: Vec::new(),
            }));
            return;
        }
    };

    log::debug!(
        "Opportunity found starting with pair {}. BBO ROI: {}, blended ROI: {}, vol: {}, trigger: {}",
        pair1_name,
        roi,
        depth.blended_roi,
        depth.volume,
        trigger.as_str()
    );

    let pair1_amount_in = depth.expected_cost;
    let volume_stable = compute_volume_stable(
        depth.volume,
        depth.vwap_bid,
        pair2_stable,
        fee_spot,
        fee_stablecoin,
    );

    let updated_pair_kraken_ts = if updated_pair_idx == pair1_idx {
        pair1.kraken_ts
    } else if updated_pair_idx == pair2_idx {
        pair2.kraken_ts
    } else {
        log::error!("Updated pair idx doesn't match pair1 or pair2 idx. Using pair1 kraken ts.");
        pair1.kraken_ts
    };

    let decision = if let Some(reason) =
        guardrail_failure_reason(depth.volume, depth.vwap_ask, depth.vwap_bid, pair1, pair2)
    {
        reason
    } else {
        let send_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        // Trade first — forensics after handoff.
        send_trade_command(
            TradeCommand::Arb(OrderInfo {
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
                opportunity_id,
                planned_vwap_ask: depth.vwap_ask,
                planned_vwap_bid: depth.vwap_bid,
                quote2_to_quote1_fx: quote2_to_quote1_fx(pair1_stable, pair2_stable),
            }),
            pair1_name,
            &trade_tx,
        )
    };

    // Influx summary (existing dashboards) — after trade handoff attempt.
    let l1_limiting_volume = pair1.ask_volume.min(pair2.bid_volume);
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
        trigger.as_str(),
        depth.vwap_ask,
        depth.vwap_bid,
        depth.blended_roi,
        depth.limit_buy_price,
        l1_limiting_volume,
        depth.expected_pnl,
    );

    try_log(ForensicsEvent::ArbOpportunity(ArbOpportunityEvent {
        event: "arb_opportunity",
        opportunity_id,
        trigger: trigger.as_str(),
        pair1: pair1_name,
        pair2: pair2_name,
        bbo_roi: roi,
        blended_roi: depth.blended_roi,
        depth_volume: depth.volume,
        l1_limiting_volume,
        depth_multiplier: if l1_limiting_volume > 0.0 {
            depth.volume / l1_limiting_volume
        } else {
            0.0
        },
        vwap_ask: depth.vwap_ask,
        vwap_bid: depth.vwap_bid,
        limit_buy_price: depth.limit_buy_price,
        expected_cost: depth.expected_cost,
        expected_proceeds: depth.expected_proceeds,
        expected_pnl: depth.expected_pnl,
        expected_pnl_bps: depth.expected_pnl_bps(),
        stop_reason: depth.stop_reason,
        balance_limited: depth.balance_limited,
        decision,
        kraken_ts: updated_pair_kraken_ts,
        eval_ts_ns,
        slices: depth.slices_vec(),
    }));
}

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

/// Hand a command to the trading thread. Returns decision string for forensics.
fn send_trade_command(
    command: TradeCommand,
    pair_name: &'static str,
    trade_tx: &mpsc::Sender<TradeCommand>,
) -> &'static str {
    if TRADER_BUSY.load(Ordering::Relaxed) {
        log::info!("Trader busy, dropping order for {}", pair_name);
        return "trader_busy";
    }

    match trade_tx.try_send(command) {
        Ok(()) => "sent",
        Err(mpsc::error::TrySendError::Full(_)) => {
            log::warn!("Channel buffer full, dropping order for {}", pair_name);
            "channel_full"
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            log::error!("Trading channel closed, cannot send order");
            "channel_closed"
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

    fn book_from_levels(asks: &[(&str, &str)], bids: &[(&str, &str)]) -> OrderBook {
        let ask_levels: Vec<(&str, &str, f64)> =
            asks.iter().map(|&(p, v)| (p, v, 1.0)).collect();
        let bid_levels: Vec<(&str, &str, f64)> =
            bids.iter().map(|&(p, v)| (p, v, 1.0)).collect();
        let mut book = OrderBook::new();
        book.apply_snapshot(&ask_levels, &bid_levels);
        book
    }

    const ARB_FEE: f64 = 0.992;
    const FEE_SPOT: f64 = 0.004;

    fn knobs() -> WalkKnobs {
        WalkKnobs {
            max_depth: BOOK_DEPTH,
            haircut: 1.0,
            roi_floor: 1.0,
        }
    }

    #[test]
    fn sibling_pairs_are_adjacent() {
        assert_eq!(sibling_pair_idx(2), 3);
        assert_eq!(sibling_pair_idx(3), 2);
        assert_eq!(sibling_pair_idx(4), 5);
        assert_eq!(sibling_pair_idx(5), 4);
    }

    #[test]
    fn dual_walk_crosses_multiple_levels_both_sides() {
        let book_buy = book_from_levels(&[("100.0", "1.0"), ("100.5", "2.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "2.0"), ("104.0", "2.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let fill = dual_walk(
            &book_buy, &book_sell, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE, knobs(),
        )
        .expect("fill");

        // All 3 units of ask volume cross profitably (deepest pair 100.5/104
        // has ROI 104/100.5 * 0.992 = 1.0265 > 1).
        assert!((fill.volume - 3.0).abs() < 1e-9);
        assert!((fill.limit_buy_price - 100.5).abs() < 1e-9);
        assert!((fill.vwap_ask - (100.0 + 2.0 * 100.5) / 3.0).abs() < 1e-9);
        assert!((fill.vwap_bid - (2.0 * 105.0 + 104.0) / 3.0).abs() < 1e-9);
        assert_eq!(fill.stop_reason, "book_depth_exhausted");
        // Two ask levels and two bid levels touched.
        let ask_slices: Vec<_> = fill
            .slices_vec()
            .into_iter()
            .filter(|s| s.leg == "walk_ask")
            .collect();
        let bid_slices: Vec<_> = fill
            .slices_vec()
            .into_iter()
            .filter(|s| s.leg == "walk_bid")
            .collect();
        assert_eq!(ask_slices.len(), 2);
        assert_eq!(bid_slices.len(), 2);
        assert!((ask_slices[1].planned_volume - 2.0).abs() < 1e-9);
        assert!((bid_slices[1].planned_volume - 1.0).abs() < 1e-9);
        assert!(fill.expected_pnl > 0.0);
    }

    #[test]
    fn dual_walk_stops_at_unprofitable_pair() {
        // Second bid level is below breakeven vs the top ask.
        let book_buy = book_from_levels(&[("100.0", "10.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "2.0"), ("100.5", "5.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let fill = dual_walk(
            &book_buy, &book_sell, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE, knobs(),
        )
        .expect("fill");

        assert!((fill.volume - 2.0).abs() < 1e-9);
        assert_eq!(fill.stop_reason, "unprofitable_level");
        assert!((fill.limit_buy_price - 100.0).abs() < 1e-9);
    }

    #[test]
    fn dual_walk_roi_buffer_tightens_stop() {
        // 104-bid level has ROI ~1.0317; a 400 bps buffer excludes it while
        // the 105-bid level (ROI ~1.0416) still passes.
        let book_buy = book_from_levels(&[("100.0", "10.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "1.0"), ("104.0", "5.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let tight = WalkKnobs {
            roi_floor: 1.04,
            ..knobs()
        };
        let fill = dual_walk(
            &book_buy, &book_sell, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE, tight,
        )
        .expect("fill");

        assert!((fill.volume - 1.0).abs() < 1e-9);
        assert_eq!(fill.stop_reason, "unprofitable_level");
    }

    #[test]
    fn dual_walk_haircuts_levels_beyond_top() {
        let book_buy = book_from_levels(&[("100.0", "1.0"), ("100.5", "2.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "10.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let haircut = WalkKnobs {
            haircut: 0.5,
            ..knobs()
        };
        let fill = dual_walk(
            &book_buy, &book_sell, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE, haircut,
        )
        .expect("fill");

        // L0 in full (1.0) + 50% of L1's 2.0 = 2.0 total.
        assert!((fill.volume - 2.0).abs() < 1e-9);
    }

    #[test]
    fn dual_walk_respects_depth_cap() {
        let book_buy = book_from_levels(&[("100.0", "1.0"), ("100.5", "2.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "10.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let capped = WalkKnobs {
            max_depth: 1,
            ..knobs()
        };
        let fill = dual_walk(
            &book_buy, &book_sell, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE, capped,
        )
        .expect("fill");

        assert!((fill.volume - 1.0).abs() < 1e-9);
        assert_eq!(fill.stop_reason, "depth_cap_reached");
    }

    #[test]
    fn dual_walk_balance_limited() {
        let book_buy = book_from_levels(&[("100.0", "10.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "10.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        let balance = 150.0;
        let fill = dual_walk(
            &book_buy, &book_sell, &s1, &s2, balance, FEE_SPOT, ARB_FEE, knobs(),
        )
        .expect("fill");

        let max_vol = balance / (100.0 * (1.0 + FEE_SPOT));
        assert!((fill.volume - max_vol).abs() < 1e-9);
        assert!(fill.balance_limited);
        assert_eq!(fill.stop_reason, "balance_capped");
    }

    #[test]
    fn dual_walk_zero_balance_returns_none() {
        let book_buy = book_from_levels(&[("100.0", "10.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "10.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(1.0, 1.0);

        assert!(dual_walk(&book_buy, &book_sell, &s1, &s2, 0.0, FEE_SPOT, ARB_FEE, knobs())
            .is_none());
    }

    #[test]
    fn check_guardrails_uses_vwap() {
        let pair1 = coin_pair();
        let pair2 = coin_pair();
        assert!(guardrail_failure_reason(1.0, 101.0, 100.0, &pair1, &pair2).is_none());
        assert_eq!(
            guardrail_failure_reason(0.0001, 101.0, 100.0, &pair1, &pair2),
            Some("below_min_order")
        );
    }

    #[test]
    fn expected_pnl_is_in_pair1_quote_via_stables() {
        let book_buy = book_from_levels(&[("100.0", "1.0")], &[]);
        let book_sell = book_from_levels(&[], &[("105.0", "1.0")]);
        let s1 = stable_pair(1.0, 1.0);
        let s2 = stable_pair(0.9, 0.9);

        let fill = dual_walk(
            &book_buy, &book_sell, &s1, &s2, 1_000_000.0, FEE_SPOT, ARB_FEE, knobs(),
        )
        .expect("fill");

        let expected_cost = 1.0 * 100.0 * (1.0 + FEE_SPOT);
        let expected_proceeds = 1.0 * 105.0 * (1.0 - FEE_SPOT) * 1.0 / 0.9;
        assert!((fill.expected_cost - expected_cost).abs() < 1e-9);
        assert!((fill.expected_proceeds - expected_proceeds).abs() < 1e-9);
        assert!((fill.expected_pnl - (expected_proceeds - expected_cost)).abs() < 1e-9);
    }
}
