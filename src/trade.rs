use crate::arb_forensics::{
    try_log, ArbExecutionEvent, FillRecord, ForensicsEvent, MakerEvent, MomentumExecutionEvent,
};
use crate::influx::{log_momentum_execution, log_trade_message_receive_speed};
use crate::maker::{
    blacklist_pair, drain_changed, global_inventory_basis, halt_maker, inventory_coin,
    invalidate_pair, is_blacklisted, last_fair_mid, maker_halted, maker_wake, record_fill,
    session_realized_pnl, set_live_ask, set_live_bid, volume_drifted, DesiredQuote,
    POST_FRESHNESS_MS,
};
use crate::structs::{MomentumOrder, OrderInfo, TradeCommand};
use crate::utils::{send_telegram_message, wait_approx_ms};
use crate::{MAKER_ENABLED, MAKER_GLOBAL_NOTIONAL, MAKER_MAX_LOSS, TRADER_BUSY};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Dead-man's-switch: Kraken cancels all resting orders if this many seconds
/// pass without a re-arm. Every crash mode flattens the book automatically.
const DMS_TIMEOUT_SECS: u64 = 15;
/// Re-arm cadence; must be comfortably below `DMS_TIMEOUT_SECS`.
const DMS_REARM_SECS: u64 = 5;

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

#[derive(Clone, Debug)]
struct OwnTradeFill {
    userref: i32,
    pair: String,
    volume: f64,
    price: f64,
    fee: f64,
    cost: f64,
    side: &'static str,
    time: f64,
}

/// `addOrderStatus` / `cancelOrderStatus` correlated back via `reqid`
/// (we set `reqid = userref` on every request we send).
#[derive(Clone, Debug)]
struct OrderStatus {
    event: String,
    reqid: i32,
    ok: bool,
    error: String,
}

#[derive(Clone, Debug)]
struct RestingQuote {
    userref: i32,
    price: f64,
    volume: f64,
}

#[derive(Default)]
struct MakerPairState {
    bid: Option<RestingQuote>,
    ask: Option<RestingQuote>,
    /// No posts on this pair until then (set after rejections so a
    /// reject → re-evaluate → repost loop can't spam the exchange).
    cooldown_until_ns: u128,
}

/// Global post pause (ns since epoch), set when Kraken reports
/// "Exceeded msg rate". All posting stops until it elapses; cancels
/// still go through (they reduce risk and rarely rate-limit).
static POST_PAUSE_UNTIL_NS: AtomicU64 = AtomicU64::new(0);

/// Cooldowns per rejection class.
const COOLDOWN_INSUFFICIENT_FUNDS: Duration = Duration::from_secs(60);
const COOLDOWN_MSG_RATE_PAIR: Duration = Duration::from_secs(10);
const COOLDOWN_DEFAULT: Duration = Duration::from_secs(2);
const GLOBAL_PAUSE_MSG_RATE: Duration = Duration::from_secs(3);

/// How a rejection should be handled going forward.
enum RejectClass {
    /// Untradeable this session (regional restriction, cancel-only market).
    Permanent,
    /// Back off this pair for a while.
    Cooldown(Duration),
    /// Back off globally: we are sending too many messages.
    RateLimited,
}

fn classify_reject(error: &str) -> RejectClass {
    if error.contains("Invalid permissions")
        || error.contains("cancel_only")
        || error.contains("restricted")
    {
        RejectClass::Permanent
    } else if error.contains("Exceeded msg rate") || error.contains("Rate limit") {
        RejectClass::RateLimited
    } else if error.contains("Insufficient funds") {
        RejectClass::Cooldown(COOLDOWN_INSUFFICIENT_FUNDS)
    } else {
        // Includes post-only "would cross" rejects: the next evaluation will
        // derive a fresh price, but give the book a beat to move first.
        RejectClass::Cooldown(COOLDOWN_DEFAULT)
    }
}

fn posts_paused(now: u128) -> bool {
    (POST_PAUSE_UNTIL_NS.load(Ordering::Relaxed) as u128) > now
}

/// Sum of price×volume across all resting bids, excluding `except_pair`
/// (whose bid is being replaced by the caller).
fn open_bid_notional(
    maker_state: &FxHashMap<&'static str, MakerPairState>,
    except_pair: &str,
) -> f64 {
    maker_state
        .iter()
        .filter(|(pair, _)| **pair != except_pair)
        .filter_map(|(_, s)| s.bid.as_ref().map(|q| q.price * q.volume))
        .sum()
}

/// Positive, non-zero userref (Kraken accepts int32; keep it positive so it
/// round-trips cleanly through `reqid` and string fields).
fn new_userref() -> i32 {
    ((rand::random::<u32>() >> 1).max(1)) as i32
}

/// Latency context captured once when the trade thread receives a command.
#[derive(Clone, Copy, Debug)]
struct ExecTiming {
    event_ts_ns: u128,
    data_age_ms: f64,
    channel_delay_ms: f64,
}

impl ExecTiming {
    fn at_gate(send_timestamp: u128, receive_timestamp: u128, data_age_s: f64) -> Self {
        Self {
            event_ts_ns: receive_timestamp,
            data_age_ms: data_age_s * 1000.0,
            channel_delay_ms: (receive_timestamp.saturating_sub(send_timestamp)) as f64 / 1e6,
        }
    }
}

/// Set up private WebSocket connection and subscribe to own trades
async fn setup_private_websocket(
    token: &str,
    ws_url: &str,
) -> Result<
    (
        SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ),
    String,
> {
    let url = url::Url::parse(ws_url)
        .map_err(|e| format!("Failed to parse WebSocket URL '{}': {}", ws_url, e))?;

    let (ws_stream, _) = connect_async(url)
        .await
        .map_err(|e| format!("Failed to connect to private websocket: {}", e))?;

    let (mut write, read) = ws_stream.split();

    // snapshot: false — Kraken otherwise replays the 50 most recent
    // historical trades on subscribe, which would poison maker inventory
    // tracking with fills from previous runs.
    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {
            "name": "ownTrades",
            "token": token,
            "snapshot": false
        }
    });

    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .map_err(|e| format!("Failed to send subscription message: {}", e))?;

    log::info!("Subscribed to own trades via private websocket.");

    Ok((write, read))
}

/// Trading thread main loop
pub async fn run_trading_thread(
    token: String,
    private_ws_url: String,
    mut trade_rx: mpsc::Receiver<TradeCommand>,
    allow_trades: bool,
) {
    if !allow_trades {
        // Eval-only: consume commands so senders never block. Maker desires
        // are already logged at publish time by the evaluator.
        while let Some(command) = trade_rx.recv().await {
            let send_timestamp = match &command {
                TradeCommand::Arb(order) => order.send_timestamp,
                TradeCommand::Momentum(order) => order.send_timestamp,
            };
            let receive_timestamp = now_ns();
            log_trade_message_receive_speed(send_timestamp, receive_timestamp);
        }
        log::info!("Trading channel closed, exiting trading thread");
        return;
    }

    let (mut write, read) = match setup_private_websocket(&token, &private_ws_url).await {
        Ok(streams) => streams,
        Err(e) => {
            let msg = format!(
                "Failed to set up private websocket: {}. Trading thread cannot continue.",
                e
            );
            log::error!("{}", msg);
            panic!("{}", msg);
        }
    };

    let (fills_tx, mut fills_rx) = mpsc::unbounded_channel::<OwnTradeFill>();
    let (status_tx, mut status_rx) = mpsc::unbounded_channel::<OrderStatus>();
    tokio::spawn(async move {
        listen_to_private_ws(read, fills_tx, status_tx).await;
    });

    let maker_on = MAKER_ENABLED.load(Ordering::Relaxed);
    let mut maker_state: FxHashMap<&'static str, MakerPairState> = FxHashMap::default();
    let mut last_seen_seq = 0u64;
    let mut dms_interval = tokio::time::interval(Duration::from_secs(DMS_REARM_SECS));

    if maker_on {
        // A previous run may have died with quotes resting: flatten before
        // quoting, and arm the dead-man's switch before the first post.
        cancel_all_orders(&mut write, &token).await;
        arm_dead_mans_switch(&mut write, &token).await;
        send_telegram_message("📌 Maker live: cancelled leftovers, dead-man's switch armed").await;
    }

    enum LoopEvent {
        Cmd(Option<TradeCommand>),
        Fill(Option<OwnTradeFill>),
        Status(Option<OrderStatus>),
        MakerWake,
        DmsRearm,
    }

    loop {
        let event = tokio::select! {
            cmd = trade_rx.recv() => LoopEvent::Cmd(cmd),
            fill = fills_rx.recv() => LoopEvent::Fill(fill),
            status = status_rx.recv() => LoopEvent::Status(status),
            _ = maker_wake().notified(), if maker_on => LoopEvent::MakerWake,
            _ = dms_interval.tick(), if maker_on => LoopEvent::DmsRearm,
        };

        match event {
            LoopEvent::Cmd(None) => {
                log::info!("Trading channel closed, exiting trading thread");
                break;
            }
            LoopEvent::Cmd(Some(command)) => {
                TRADER_BUSY.store(true, Ordering::Relaxed);
                handle_trade_command(&mut write, &token, command, &mut fills_rx).await;
                TRADER_BUSY.store(false, Ordering::Relaxed);
            }
            LoopEvent::Fill(None) => {
                log::error!("Private websocket fill stream ended; exiting trading thread");
                break;
            }
            LoopEvent::Fill(Some(fill)) => {
                if maker_on {
                    apply_maker_fill(&fill, &mut maker_state);
                    enforce_risk_limits(&mut write, &token, &mut maker_state).await;
                }
            }
            LoopEvent::Status(None) => {
                log::error!("Private websocket status stream ended; exiting trading thread");
                break;
            }
            LoopEvent::Status(Some(status)) => {
                if maker_on {
                    apply_order_status(&status, &mut maker_state);
                }
            }
            LoopEvent::MakerWake => {
                for desire in drain_changed(&mut last_seen_seq) {
                    reconcile_maker_pair(&mut write, &token, &desire, &mut maker_state).await;
                }
            }
            LoopEvent::DmsRearm => {
                arm_dead_mans_switch(&mut write, &token).await;
            }
        }
    }

    if maker_on {
        // Best-effort flatten; the dead-man's switch covers the failure case.
        cancel_all_orders(&mut write, &token).await;
    }
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

/// Execute one arb or momentum command (staleness-gated).
async fn handle_trade_command(
    write: &mut WsSink,
    token: &str,
    command: TradeCommand,
    fills_rx: &mut mpsc::UnboundedReceiver<OwnTradeFill>,
) {
    let (send_timestamp, updated_pair_kraken_ts) = match &command {
        TradeCommand::Arb(order) => (order.send_timestamp, order.updated_pair_kraken_ts),
        TradeCommand::Momentum(order) => (order.send_timestamp, order.updated_pair_kraken_ts),
    };
    let receive_timestamp = now_ns();
    log_trade_message_receive_speed(send_timestamp, receive_timestamp);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let time_diff = now - updated_pair_kraken_ts;
    // Temporary 10ms gate (was 1.5ms) to measure slippage vs data age.
    let fresh = time_diff < 0.010;
    let timing = ExecTiming::at_gate(send_timestamp, receive_timestamp, time_diff);

    match command {
        TradeCommand::Arb(order) => {
            if fresh {
                log::debug!("Sending order starting with {}", order.pair1_name);
                make_trades_limit_ioc(write, token, &order, fills_rx, timing).await;
            } else {
                log::warn!(
                    "Skipping trade for {}: data too stale (time_diff={}s)",
                    order.pair1_name,
                    time_diff,
                );
                try_log(ForensicsEvent::ArbExecution(ArbExecutionEvent {
                    event: "arb_execution",
                    opportunity_id: order.opportunity_id,
                    userref: 0,
                    pair1: order.pair1_name,
                    pair2: order.pair2_name,
                    requested_volume: order.volume_coin,
                    limit_buy_price: order.pair1_price,
                    planned_vwap_ask: order.planned_vwap_ask,
                    planned_vwap_bid: order.planned_vwap_bid,
                    buy_fills: Vec::new(),
                    sell_fills: Vec::new(),
                    actual_buy_volume: 0.0,
                    actual_buy_vwap: 0.0,
                    actual_buy_fee: 0.0,
                    actual_sell_volume: 0.0,
                    actual_sell_vwap: 0.0,
                    actual_sell_fee: 0.0,
                    volume_shortfall: order.volume_coin,
                    buy_slippage_bps: 0.0,
                    sell_slippage_bps: 0.0,
                    realized_pnl: 0.0,
                    outcome: "stale_skip",
                    event_ts_ns: timing.event_ts_ns,
                    data_age_ms: timing.data_age_ms,
                    channel_delay_ms: timing.channel_delay_ms,
                }));
            }
        }
        TradeCommand::Momentum(order) => {
            if fresh {
                log::debug!("Sending momentum order for {}", order.pair_name);
                make_momentum_trade(write, token, &order, fills_rx, timing).await;
            } else {
                log::warn!(
                    "Skipping momentum trade for {}: data too stale (time_diff={}s)",
                    order.pair_name,
                    time_diff,
                );
                log_momentum_outcome(&order, 0, &[], &[], "stale_skip", timing);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Maker execution
// ---------------------------------------------------------------------------

fn maker_event_base(pair: String, event: &'static str) -> MakerEvent {
    MakerEvent {
        event,
        opportunity_id: 0,
        pair,
        sibling: "",
        side: "none",
        fair_mid: 0.0,
        bid_price: 0.0,
        bid_volume: 0.0,
        ask_price: 0.0,
        ask_volume: 0.0,
        price: 0.0,
        volume: 0.0,
        userref: 0,
        inventory_coin: 0.0,
        fee: 0.0,
        realized_pnl: 0.0,
        session_pnl: session_realized_pnl(),
        reason: String::new(),
        event_ts_ns: now_ns(),
    }
}

/// Apply an ownTrades fill: position/PnL first (unconditionally — a fill that
/// raced our cancel still changes our position), then resting-quote state.
fn apply_maker_fill(fill: &OwnTradeFill, maker_state: &mut FxHashMap<&'static str, MakerPairState>) {
    let realized_pnl = record_fill(
        &fill.pair,
        fill.side == "buy",
        fill.price,
        fill.volume,
        fill.fee,
    );

    // Shrink the resting quote if this fill matches one we still track.
    if let Some((pair_key, state)) = maker_state
        .iter_mut()
        .find(|(pair, _)| **pair == fill.pair)
    {
        let pair_key = *pair_key;
        let slot = if fill.side == "buy" {
            &mut state.bid
        } else {
            &mut state.ask
        };
        if let Some(q) = slot {
            if q.userref == fill.userref {
                let remaining = q.volume - fill.volume;
                if remaining <= 1e-12 {
                    *slot = None;
                } else {
                    q.volume = remaining;
                }
                let live = slot.as_ref().map(|q| (q.price, q.volume));
                if fill.side == "buy" {
                    set_live_bid(pair_key, live);
                } else {
                    set_live_ask(pair_key, live);
                }
            }
        }
    }

    let inv = inventory_coin(&fill.pair);
    let mut fill_event = maker_event_base(fill.pair.clone(), "maker_fill");
    fill_event.side = fill.side;
    // Fair mid from the most recent desire: lets offline analysis measure
    // adverse selection (fill price vs fair at fill time) per pair.
    fill_event.fair_mid = last_fair_mid(&fill.pair);
    fill_event.price = fill.price;
    fill_event.volume = fill.volume;
    fill_event.userref = fill.userref;
    fill_event.inventory_coin = inv;
    fill_event.fee = fill.fee;
    fill_event.realized_pnl = realized_pnl;
    fill_event.reason = "own_trades".to_string();
    try_log(ForensicsEvent::Maker(fill_event));

    let mut inv_event = maker_event_base(fill.pair.clone(), "maker_inventory");
    inv_event.volume = inv;
    inv_event.inventory_coin = inv;
    inv_event.reason = "post_fill".to_string();
    try_log(ForensicsEvent::Maker(inv_event));

    // Live state changed underneath the registry: drop the stored desire so
    // the evaluator's next publish for this pair can't be coalesced away.
    invalidate_pair(&fill.pair);
}

/// Apply an `addOrderStatus`/`cancelOrderStatus` reply. Rejected posts (e.g.
/// post-only would cross, insufficient funds) must clear our optimistic
/// resting-quote state or that side would stay dark until the price moved.
fn apply_order_status(
    status: &OrderStatus,
    maker_state: &mut FxHashMap<&'static str, MakerPairState>,
) {
    if status.ok {
        return;
    }
    if status.event == "cancelOrderStatus" {
        // Typically "Unknown order": it filled or was already gone. Fills
        // arrive separately via ownTrades, so state is already correct.
        log::debug!("Cancel reqid {} failed: {}", status.reqid, status.error);
        return;
    }

    let class = classify_reject(&status.error);
    if matches!(class, RejectClass::RateLimited) {
        // Pause all posting even if we can't map the reqid to a pair.
        let until = now_ns() as u64 + GLOBAL_PAUSE_MSG_RATE.as_nanos() as u64;
        if POST_PAUSE_UNTIL_NS.fetch_max(until, Ordering::Relaxed) < until {
            log::warn!(
                "Kraken msg rate exceeded; pausing maker posts for {:?}",
                GLOBAL_PAUSE_MSG_RATE
            );
        }
    }

    for (pair, state) in maker_state.iter_mut() {
        let (slot, side) = if state
            .bid
            .as_ref()
            .is_some_and(|q| q.userref == status.reqid)
        {
            (&mut state.bid, "buy")
        } else if state
            .ask
            .as_ref()
            .is_some_and(|q| q.userref == status.reqid)
        {
            (&mut state.ask, "sell")
        } else {
            continue;
        };

        let rejected = slot.take().expect("slot checked above");
        if side == "buy" {
            set_live_bid(pair, None);
        } else {
            set_live_ask(pair, None);
        }
        // Force the evaluator's next publish through to the reconciler; the
        // rejected side needs a fresh (re-derived) price, not a coalesced skip.
        invalidate_pair(pair);
        match &class {
            RejectClass::Permanent => {
                if !is_blacklisted(pair) {
                    log::info!(
                        "Blacklisting {} for this session ({} {} rejected: {})",
                        pair,
                        side,
                        status.reqid,
                        status.error
                    );
                    blacklist_pair(pair);
                }
            }
            RejectClass::Cooldown(cooldown) => {
                state.cooldown_until_ns = now_ns() + cooldown.as_nanos();
                log::debug!(
                    "Maker {} on {} rejected (userref {}), cooling {:?}: {}",
                    side,
                    pair,
                    status.reqid,
                    cooldown,
                    status.error
                );
            }
            RejectClass::RateLimited => {
                state.cooldown_until_ns = now_ns() + COOLDOWN_MSG_RATE_PAIR.as_nanos();
            }
        }
        let mut event = maker_event_base(pair.to_string(), "maker_reject");
        event.side = side;
        event.price = rejected.price;
        event.volume = rejected.volume;
        event.userref = rejected.userref;
        event.inventory_coin = inventory_coin(pair);
        event.reason = status.error.clone();
        try_log(ForensicsEvent::Maker(event));
        return;
    }
    // Usually a reject arriving after we already replaced the slot;
    // nothing to clean up.
    log::debug!(
        "addOrderStatus error for unknown reqid {}: {}",
        status.reqid,
        status.error
    );
}

/// Post-fill risk checks, in order of severity:
/// 1. Session loss limit → latch the kill switch, cancel everything, telegram.
/// 2. Global inventory cap → pull every resting bid immediately (the
///    evaluator also stops desiring bids, but don't wait for its next tick).
async fn enforce_risk_limits(
    write: &mut WsSink,
    token: &str,
    maker_state: &mut FxHashMap<&'static str, MakerPairState>,
) {
    let pnl = session_realized_pnl();
    let max_loss = MAKER_MAX_LOSS.load(Ordering::Relaxed).max(1) as f64;
    if pnl <= -max_loss && !maker_halted() {
        halt_maker();
        cancel_all_orders(write, token).await;
        for (pair, state) in maker_state.iter_mut() {
            state.bid = None;
            state.ask = None;
            set_live_bid(pair, None);
            set_live_ask(pair, None);
        }
        let msg = format!(
            "🛑 Maker halted: session realized PnL ${:.2} breached -${:.0} limit. All orders cancelled; quoting stopped.",
            pnl, max_loss
        );
        log::error!("{}", msg);
        let mut event = maker_event_base("ALL".to_string(), "maker_halt");
        event.reason = format!("session_pnl {:.2} <= -{:.0}", pnl, max_loss);
        try_log(ForensicsEvent::Maker(event));
        send_telegram_message(&msg).await;
        return;
    }

    let global_cap = MAKER_GLOBAL_NOTIONAL.load(Ordering::Relaxed).max(1) as f64;
    let basis = global_inventory_basis();
    if basis >= global_cap {
        for (pair, state) in maker_state.iter_mut() {
            if let Some(q) = state.bid.take() {
                set_live_bid(pair, None);
                cancel_userref(write, token, q.userref).await;
                let mut event = maker_event_base(pair.to_string(), "maker_cancel");
                event.side = "buy";
                event.price = q.price;
                event.volume = q.volume;
                event.userref = q.userref;
                event.inventory_coin = inventory_coin(pair);
                event.reason = format!("global_cap basis {:.2} >= {:.0}", basis, global_cap);
                try_log(ForensicsEvent::Maker(event));
                invalidate_pair(pair);
            }
        }
    }
}

async fn send_ws(write: &mut WsSink, msg: String, what: &str) -> bool {
    if let Err(e) = write.send(Message::Text(msg)).await {
        log::error!("Failed to send {}: {:?}", what, e);
        return false;
    }
    true
}

/// Cancel every open order on the account (startup/shutdown flatten).
async fn cancel_all_orders(write: &mut WsSink, token: &str) {
    let msg = serde_json::json!({
        "event": "cancelAll",
        "token": token,
    })
    .to_string();
    if send_ws(write, msg, "cancelAll").await {
        log::info!("Sent cancelAll (flatten resting orders)");
    }
}

/// (Re-)arm Kraken's server-side dead-man's switch: if we stop re-arming
/// (crash, hang, disconnect), Kraken cancels all resting orders after
/// `DMS_TIMEOUT_SECS`.
async fn arm_dead_mans_switch(write: &mut WsSink, token: &str) {
    let msg = serde_json::json!({
        "event": "cancelAllOrdersAfter",
        "token": token,
        "timeout": DMS_TIMEOUT_SECS,
    })
    .to_string();
    send_ws(write, msg, "cancelAllOrdersAfter").await;
}

async fn cancel_userref(write: &mut WsSink, token: &str, userref: i32) {
    let msg = serde_json::json!({
        "event": "cancelOrder",
        "token": token,
        "reqid": userref,
        "txid": [userref.to_string()],
    })
    .to_string();
    let _ = send_ws(write, msg, "cancelOrder").await;
}

async fn post_maker_limit(
    write: &mut WsSink,
    token: &str,
    pair: &str,
    side: &str,
    price_str: String,
    volume_str: String,
) -> Option<i32> {
    let userref = new_userref();
    let msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "reqid": userref,
        "type": side,
        "ordertype": "limit",
        "price": price_str,
        "volume": volume_str,
        "pair": pair,
        "userref": userref.to_string(),
        "oflags": "post",
    })
    .to_string();
    if !send_ws(write, msg, "maker addOrder").await {
        return None;
    }
    Some(userref)
}

/// Converge one side of a pair: cancel when price moved at least half a tick
/// or size drifted beyond tolerance, then post when allowed (fresh desire,
/// no cooldown/pause, within the open-bid budget).
#[allow(clippy::too_many_arguments)]
async fn reconcile_maker_side(
    write: &mut WsSink,
    token: &str,
    desire: &DesiredQuote,
    slot: &mut Option<RestingQuote>,
    desired: Option<(f64, f64)>,
    side: &'static str,
    allow_post: bool,
) {
    let tick = 10f64.powi(-(desire.price_decimals as i32));
    let pair = desire.pair_name;

    let need_cancel = match (&*slot, desired) {
        (Some(_), None) => true,
        (Some(q), Some((price, volume))) => {
            (q.price - price).abs() >= tick * 0.5 || volume_drifted(q.volume, volume)
        }
        _ => false,
    };
    if need_cancel {
        if let Some(q) = slot.take() {
            if side == "buy" {
                set_live_bid(pair, None);
            } else {
                set_live_ask(pair, None);
            }
            cancel_userref(write, token, q.userref).await;
            let mut event = maker_event_base(pair.to_string(), "maker_cancel");
            event.opportunity_id = desire.opportunity_id;
            event.sibling = desire.sibling_name;
            event.side = side;
            event.fair_mid = desire.fair_mid;
            event.price = q.price;
            event.volume = q.volume;
            event.userref = q.userref;
            event.inventory_coin = desire.inventory_coin;
            event.reason = "reconcile".to_string();
            try_log(ForensicsEvent::Maker(event));
        }
    }

    let Some((price, volume)) = desired else {
        return;
    };
    if slot.is_some() || !allow_post || volume <= 0.0 || maker_halted() {
        return;
    }

    let price_str = format!("{:.*}", desire.price_decimals, price);
    let volume_str = format!("{:.*}", desire.volume_decimals, volume);
    if let Some(userref) =
        post_maker_limit(write, token, pair, side, price_str, volume_str).await
    {
        *slot = Some(RestingQuote {
            userref,
            price,
            volume,
        });
        if side == "buy" {
            set_live_bid(pair, Some((price, volume)));
        } else {
            set_live_ask(pair, Some((price, volume)));
        }
        let mut event = maker_event_base(pair.to_string(), "maker_quote");
        event.opportunity_id = desire.opportunity_id;
        event.sibling = desire.sibling_name;
        event.side = side;
        event.fair_mid = desire.fair_mid;
        event.price = price;
        event.volume = volume;
        event.userref = userref;
        event.inventory_coin = desire.inventory_coin;
        event.reason = "posted".to_string();
        try_log(ForensicsEvent::Maker(event));
    }
}

async fn reconcile_maker_pair(
    write: &mut WsSink,
    token: &str,
    desire: &DesiredQuote,
    maker_state: &mut FxHashMap<&'static str, MakerPairState>,
) {
    // Cancels always run; posts only when the desire is fresh enough that
    // the price still reflects the current book, the pair isn't cooling
    // down after a rejection, and we aren't globally rate-limited.
    let now = now_ns();
    let age_ms = now.saturating_sub(desire.eval_ts_ns) as f64 / 1e6;
    let fresh = age_ms < POST_FRESHNESS_MS;
    let cooling = maker_state
        .get(desire.pair_name)
        .is_some_and(|s| s.cooldown_until_ns > now);
    let allow_post = fresh && !cooling && !posts_paused(now);

    // Bids commit new capital, so they must also fit under the global
    // notional cap alongside held inventory and every other resting bid.
    let bid_desired = desire.bid.map(|q| (q.price, q.volume));
    let bid_allowed = allow_post
        && bid_desired.is_none_or(|(price, volume)| {
            let cap = MAKER_GLOBAL_NOTIONAL.load(Ordering::Relaxed).max(1) as f64;
            let committed =
                open_bid_notional(maker_state, desire.pair_name) + global_inventory_basis();
            committed + price * volume <= cap
        });

    let state = maker_state.entry(desire.pair_name).or_default();

    // Split borrows: bid and ask reconcile independently.
    let ask_desired = desire.ask.map(|q| (q.price, q.volume));
    reconcile_maker_side(write, token, desire, &mut state.bid, bid_desired, "buy", bid_allowed)
        .await;
    let state = maker_state.entry(desire.pair_name).or_default();
    reconcile_maker_side(write, token, desire, &mut state.ask, ask_desired, "sell", allow_post)
        .await;
}

async fn listen_to_private_ws(
    mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    filled_volume_tx: mpsc::UnboundedSender<OwnTradeFill>,
    status_tx: mpsc::UnboundedSender<OrderStatus>,
) {
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(array) = data.as_array() {
                        if array.len() >= 3 && array[1].as_str() == Some("ownTrades") {
                            if let Some(trades_array) = array[0].as_array() {
                                for trade_obj in trades_array {
                                    if let Some(trade_obj_map) = trade_obj.as_object() {
                                        for (_trade_id, trade_data) in trade_obj_map {
                                            if let Some(trade_info) = trade_data.as_object() {
                                                if let Some(fill) = parse_own_trade_fill(trade_info)
                                                {
                                                    log::debug!(
                                                        "Received ownTrade fill: userref={}, pair={}, volume={}, price={}, side={}",
                                                        fill.userref,
                                                        fill.pair,
                                                        fill.volume,
                                                        fill.price,
                                                        fill.side
                                                    );
                                                    if filled_volume_tx.send(fill).is_err() {
                                                        log::warn!(
                                                            "filled_volume_tx receiver dropped"
                                                        );
                                                        return;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else if let Some(status) = parse_order_status(&data) {
                        if status_tx.send(status).is_err() {
                            log::warn!("status_tx receiver dropped");
                            return;
                        }
                    }
                }
            }
            Ok(_) => {
                log::warn!("Private websocket closed or received non-text message");
                break;
            }
            Err(e) => {
                log::error!("Error reading private websocket: {:?}", e);
                break;
            }
        }
    }
    log::info!("Private websocket listener task ended");
}

/// Parse `addOrderStatus` / `cancelOrderStatus` events. Correlation is via
/// `reqid`, which we set to the order's userref on every request.
fn parse_order_status(data: &serde_json::Value) -> Option<OrderStatus> {
    let event = data.get("event")?.as_str()?;
    if event != "addOrderStatus" && event != "cancelOrderStatus" {
        return None;
    }
    let status = data.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let reqid = data.get("reqid").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    Some(OrderStatus {
        event: event.to_string(),
        reqid,
        ok: status == "ok",
        error: data
            .get("errorMessage")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    })
}

fn parse_own_trade_fill(
    trade_info: &serde_json::Map<String, serde_json::Value>,
) -> Option<OwnTradeFill> {
    let userref = trade_info.get("userref").and_then(|v| {
        v.as_i64()
            .map(|i| i as i32)
            .or_else(|| v.as_str().and_then(|s| s.parse::<i32>().ok()))
    })?;

    let pair = trade_info
        .get("pair")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let volume = trade_info
        .get("vol")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())?;

    let price = trade_info
        .get("price")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let fee = trade_info
        .get("fee")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let cost = trade_info
        .get("cost")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(price * volume);

    let side = match trade_info.get("type").and_then(|v| v.as_str()) {
        Some("buy") | Some("b") => "buy",
        Some("sell") | Some("s") => "sell",
        _ => return None,
    };

    let time = trade_info
        .get("time")
        .and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v.as_f64())
        })
        .unwrap_or(0.0);

    Some(OwnTradeFill {
        userref,
        pair,
        volume,
        price,
        fee,
        cost,
        side,
        time,
    })
}

fn summarize_fills(fills: &[OwnTradeFill]) -> (f64, f64, f64, Vec<FillRecord>) {
    let mut volume = 0.0;
    let mut notional = 0.0;
    let mut fee = 0.0;
    let mut records = Vec::with_capacity(fills.len());
    for f in fills {
        volume += f.volume;
        notional += f.price * f.volume;
        fee += f.fee;
        records.push(FillRecord {
            volume: f.volume,
            price: f.price,
            fee: f.fee,
            cost: f.cost,
            side: f.side,
            time: f.time,
        });
    }
    let vwap = if volume > 0.0 { notional / volume } else { 0.0 };
    (volume, vwap, fee, records)
}

fn slippage_bps(planned: f64, actual: f64, higher_is_worse: bool) -> f64 {
    if planned <= 0.0 || actual <= 0.0 {
        return 0.0;
    }
    let raw = (actual - planned) / planned * 10_000.0;
    if higher_is_worse {
        raw
    } else {
        -raw
    }
}

/// Realized PnL in pair1 quote: buy cost stays in pair1; sell proceeds convert via FX.
fn realized_pnl_pair1_quote(
    buy_vwap: f64,
    buy_volume: f64,
    buy_fee: f64,
    sell_vwap: f64,
    sell_volume: f64,
    sell_fee: f64,
    quote2_to_quote1_fx: f64,
) -> f64 {
    let cost = buy_vwap * buy_volume + buy_fee;
    let proceeds_quote2 = sell_vwap * sell_volume - sell_fee;
    proceeds_quote2 * quote2_to_quote1_fx - cost
}

/// LIMIT IOC buy order, listen to ownTrades to get filled volume, then market sell
async fn make_trades_limit_ioc(
    write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    token: &str,
    order: &OrderInfo,
    filled_volume_rx: &mut mpsc::UnboundedReceiver<OwnTradeFill>,
    timing: ExecTiming,
) {
    let userref = new_userref();
    let vol_coin_formatted = format!("{:.*}", order.volume_decimals_coin, order.volume_coin);
    let limit_price_str = format!("{:.*}", order.price_decimals, order.pair1_price);

    let trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "limit",
        "price": limit_price_str,
        "volume": vol_coin_formatted,
        "pair": order.pair1_name,
        "userref": userref.to_string(),
        "timeinforce": "IOC"
    })
    .to_string();

    if let Err(e) = write.send(Message::Text(trade_msg)).await {
        log::error!(
            "Failed to send LIMIT IOC buy order for {}: {:?}",
            order.pair1_name,
            e
        );
        try_log(ForensicsEvent::ArbExecution(ArbExecutionEvent {
            event: "arb_execution",
            opportunity_id: order.opportunity_id,
            userref,
            pair1: order.pair1_name,
            pair2: order.pair2_name,
            requested_volume: order.volume_coin,
            limit_buy_price: order.pair1_price,
            planned_vwap_ask: order.planned_vwap_ask,
            planned_vwap_bid: order.planned_vwap_bid,
            buy_fills: Vec::new(),
            sell_fills: Vec::new(),
            actual_buy_volume: 0.0,
            actual_buy_vwap: 0.0,
            actual_buy_fee: 0.0,
            actual_sell_volume: 0.0,
            actual_sell_vwap: 0.0,
            actual_sell_fee: 0.0,
            volume_shortfall: order.volume_coin,
            buy_slippage_bps: 0.0,
            sell_slippage_bps: 0.0,
            realized_pnl: 0.0,
            outcome: "buy_send_failed",
            event_ts_ns: timing.event_ts_ns,
            data_age_ms: timing.data_age_ms,
            channel_delay_ms: timing.channel_delay_ms,
        }));
        return;
    }

    log::debug!(
        "Sent LIMIT IOC buy order for {} with userref {} and limit price {}",
        order.pair1_name,
        userref,
        limit_price_str
    );

    // Collect buy fills (IOC may fragment).
    let timeout_duration = Duration::from_secs(1);
    let mut buy_fills: Vec<OwnTradeFill> = Vec::new();
    let buy_result = timeout(timeout_duration, async {
        loop {
            if let Some(fill) = filled_volume_rx.recv().await {
                if fill.userref == userref && fill.side == "buy" {
                    buy_fills.push(fill);
                    // IOC typically completes quickly; keep collecting briefly via outer timeout.
                    // Return once we have at least one fill — additional fragments may arrive
                    // before sell wait; drain non-blocking after.
                    return true;
                }
            } else {
                return false;
            }
        }
    })
    .await;

    // Drain any additional buy fragments already queued.
    while let Ok(fill) = filled_volume_rx.try_recv() {
        if fill.userref == userref && fill.side == "buy" {
            buy_fills.push(fill);
        }
    }

    let (actual_buy_volume, actual_buy_vwap, actual_buy_fee, buy_records) =
        summarize_fills(&buy_fills);

    match buy_result {
        Ok(false) => {
            log::error!(
                "ownTrades channel closed before receiving fill for userref {}",
                userref
            );
            try_log(ForensicsEvent::ArbExecution(ArbExecutionEvent {
                event: "arb_execution",
                opportunity_id: order.opportunity_id,
                userref,
                pair1: order.pair1_name,
                pair2: order.pair2_name,
                requested_volume: order.volume_coin,
                limit_buy_price: order.pair1_price,
                planned_vwap_ask: order.planned_vwap_ask,
                planned_vwap_bid: order.planned_vwap_bid,
                buy_fills: buy_records,
                sell_fills: Vec::new(),
                actual_buy_volume,
                actual_buy_vwap,
                actual_buy_fee,
                actual_sell_volume: 0.0,
                actual_sell_vwap: 0.0,
                actual_sell_fee: 0.0,
                volume_shortfall: order.volume_coin - actual_buy_volume,
                buy_slippage_bps: slippage_bps(order.planned_vwap_ask, actual_buy_vwap, true),
                sell_slippage_bps: 0.0,
                realized_pnl: 0.0,
                outcome: "buy_channel_closed",
                event_ts_ns: timing.event_ts_ns,
                data_age_ms: timing.data_age_ms,
                channel_delay_ms: timing.channel_delay_ms,
            }));
            return;
        }
        Err(_) if buy_fills.is_empty() => {
            log::error!(
                "Timeout waiting for ownTrades fill confirmation for userref {}",
                userref
            );
            try_log(ForensicsEvent::ArbExecution(ArbExecutionEvent {
                event: "arb_execution",
                opportunity_id: order.opportunity_id,
                userref,
                pair1: order.pair1_name,
                pair2: order.pair2_name,
                requested_volume: order.volume_coin,
                limit_buy_price: order.pair1_price,
                planned_vwap_ask: order.planned_vwap_ask,
                planned_vwap_bid: order.planned_vwap_bid,
                buy_fills: buy_records,
                sell_fills: Vec::new(),
                actual_buy_volume: 0.0,
                actual_buy_vwap: 0.0,
                actual_buy_fee: 0.0,
                actual_sell_volume: 0.0,
                actual_sell_vwap: 0.0,
                actual_sell_fee: 0.0,
                volume_shortfall: order.volume_coin,
                buy_slippage_bps: 0.0,
                sell_slippage_bps: 0.0,
                realized_pnl: 0.0,
                outcome: "buy_timeout",
                event_ts_ns: timing.event_ts_ns,
                data_age_ms: timing.data_age_ms,
                channel_delay_ms: timing.channel_delay_ms,
            }));
            return;
        }
        _ => {}
    }

    if actual_buy_volume <= 0.0 {
        try_log(ForensicsEvent::ArbExecution(ArbExecutionEvent {
            event: "arb_execution",
            opportunity_id: order.opportunity_id,
            userref,
            pair1: order.pair1_name,
            pair2: order.pair2_name,
            requested_volume: order.volume_coin,
            limit_buy_price: order.pair1_price,
            planned_vwap_ask: order.planned_vwap_ask,
            planned_vwap_bid: order.planned_vwap_bid,
            buy_fills: buy_records,
            sell_fills: Vec::new(),
            actual_buy_volume: 0.0,
            actual_buy_vwap: 0.0,
            actual_buy_fee: 0.0,
            actual_sell_volume: 0.0,
            actual_sell_vwap: 0.0,
            actual_sell_fee: 0.0,
            volume_shortfall: order.volume_coin,
            buy_slippage_bps: 0.0,
            sell_slippage_bps: 0.0,
            realized_pnl: 0.0,
            outcome: "buy_timeout",
            event_ts_ns: timing.event_ts_ns,
            data_age_ms: timing.data_age_ms,
            channel_delay_ms: timing.channel_delay_ms,
        }));
        return;
    }

    log::debug!(
        "Received filled volume {} for userref {} on {}",
        actual_buy_volume,
        userref,
        order.pair1_name
    );

    let filled_vol_formatted = format!("{:.*}", order.volume_decimals_coin, actual_buy_volume);
    let trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "sell",
        "ordertype": "market",
        "volume": filled_vol_formatted,
        "pair": order.pair2_name,
        "userref": userref.to_string(),
    })
    .to_string();

    if let Err(e) = write.send(Message::Text(trade_msg)).await {
        log::error!(
            "Failed to send market sell order for {}: {:?}",
            order.pair2_name,
            e
        );
        try_log(ForensicsEvent::ArbExecution(ArbExecutionEvent {
            event: "arb_execution",
            opportunity_id: order.opportunity_id,
            userref,
            pair1: order.pair1_name,
            pair2: order.pair2_name,
            requested_volume: order.volume_coin,
            limit_buy_price: order.pair1_price,
            planned_vwap_ask: order.planned_vwap_ask,
            planned_vwap_bid: order.planned_vwap_bid,
            buy_fills: buy_records,
            sell_fills: Vec::new(),
            actual_buy_volume,
            actual_buy_vwap,
            actual_buy_fee,
            actual_sell_volume: 0.0,
            actual_sell_vwap: 0.0,
            actual_sell_fee: 0.0,
            volume_shortfall: order.volume_coin - actual_buy_volume,
            buy_slippage_bps: slippage_bps(order.planned_vwap_ask, actual_buy_vwap, true),
            sell_slippage_bps: 0.0,
            realized_pnl: -(actual_buy_vwap * actual_buy_volume) - actual_buy_fee,
            outcome: "sell_failed",
            event_ts_ns: timing.event_ts_ns,
            data_age_ms: timing.data_age_ms,
            channel_delay_ms: timing.channel_delay_ms,
        }));
        return;
    }

    // Collect sell fills briefly.
    let mut sell_fills: Vec<OwnTradeFill> = Vec::new();
    let sell_timeout = Duration::from_millis(500);
    let _ = timeout(sell_timeout, async {
        while let Some(fill) = filled_volume_rx.recv().await {
            if fill.userref == userref && fill.side == "sell" {
                sell_fills.push(fill);
            }
        }
    })
    .await;
    while let Ok(fill) = filled_volume_rx.try_recv() {
        if fill.userref == userref && fill.side == "sell" {
            sell_fills.push(fill);
        }
    }

    let (actual_sell_volume, actual_sell_vwap, actual_sell_fee, sell_records) =
        summarize_fills(&sell_fills);

    let volume_shortfall =
        (order.volume_coin - actual_buy_volume.min(actual_sell_volume)).max(0.0);
    let buy_slippage_bps = slippage_bps(order.planned_vwap_ask, actual_buy_vwap, true);
    // For sells, lower price is worse.
    let sell_slippage_bps = slippage_bps(order.planned_vwap_bid, actual_sell_vwap, false);
    let realized_pnl = realized_pnl_pair1_quote(
        actual_buy_vwap,
        actual_buy_volume,
        actual_buy_fee,
        actual_sell_vwap,
        actual_sell_volume,
        actual_sell_fee,
        order.quote2_to_quote1_fx,
    );

    let outcome = if actual_sell_volume <= 0.0 {
        "sell_failed"
    } else if actual_buy_volume + 1e-12 < order.volume_coin {
        "partial_buy"
    } else {
        "filled"
    };

    log::debug!(
        "Successfully completed LIMIT IOC trades for arbitrage starting with {}",
        order.pair1_name
    );

    try_log(ForensicsEvent::ArbExecution(ArbExecutionEvent {
        event: "arb_execution",
        opportunity_id: order.opportunity_id,
        userref,
        pair1: order.pair1_name,
        pair2: order.pair2_name,
        requested_volume: order.volume_coin,
        limit_buy_price: order.pair1_price,
        planned_vwap_ask: order.planned_vwap_ask,
        planned_vwap_bid: order.planned_vwap_bid,
        buy_fills: buy_records,
        sell_fills: sell_records,
        actual_buy_volume,
        actual_buy_vwap,
        actual_buy_fee,
        actual_sell_volume,
        actual_sell_vwap,
        actual_sell_fee,
        volume_shortfall,
        buy_slippage_bps,
        sell_slippage_bps,
        realized_pnl,
        outcome,
        event_ts_ns: timing.event_ts_ns,
        data_age_ms: timing.data_age_ms,
        channel_delay_ms: timing.channel_delay_ms,
    }));

    wait_approx_ms(500).await;
}

/// Wait for the first matching fill (bounded by `timeout_duration`), then
/// drain any queued fragments. Returns (fills, channel_closed).
async fn collect_fills(
    filled_volume_rx: &mut mpsc::UnboundedReceiver<OwnTradeFill>,
    userref: i32,
    side: &'static str,
    timeout_duration: Duration,
) -> (Vec<OwnTradeFill>, bool) {
    let mut fills: Vec<OwnTradeFill> = Vec::new();
    let result = timeout(timeout_duration, async {
        loop {
            match filled_volume_rx.recv().await {
                Some(fill) => {
                    if fill.userref == userref && fill.side == side {
                        fills.push(fill);
                        return true;
                    }
                }
                None => return false,
            }
        }
    })
    .await;
    let channel_closed = matches!(result, Ok(false));

    while let Ok(fill) = filled_volume_rx.try_recv() {
        if fill.userref == userref && fill.side == side {
            fills.push(fill);
        }
    }
    (fills, channel_closed)
}

/// Emit momentum forensics + Influx from whatever fills we have.
fn log_momentum_outcome(
    order: &MomentumOrder,
    userref: i32,
    buy_fills: &[OwnTradeFill],
    sell_fills: &[OwnTradeFill],
    outcome: &'static str,
    timing: ExecTiming,
) {
    let (buy_volume, buy_vwap, buy_fee, buy_records) = summarize_fills(buy_fills);
    let (sell_volume, sell_vwap, sell_fee, sell_records) = summarize_fills(sell_fills);
    // Same-currency round trip: no FX conversion.
    let realized_pnl = (sell_vwap * sell_volume - sell_fee) - (buy_vwap * buy_volume + buy_fee);

    log_momentum_execution(
        order.pair_name,
        order.trigger_pair_name,
        order.gap_bps,
        order.hold_ms,
        order.volume_coin,
        order.limit_buy_price,
        buy_volume,
        buy_vwap,
        buy_fee,
        sell_volume,
        sell_vwap,
        sell_fee,
        realized_pnl,
        outcome,
    );

    try_log(ForensicsEvent::MomentumExecution(MomentumExecutionEvent {
        event: "momentum_execution",
        opportunity_id: order.opportunity_id,
        userref,
        pair: order.pair_name,
        trigger_pair: order.trigger_pair_name,
        gap_bps: order.gap_bps,
        hold_ms: order.hold_ms,
        requested_volume: order.volume_coin,
        limit_buy_price: order.limit_buy_price,
        buy_fills: buy_records,
        sell_fills: sell_records,
        actual_buy_volume: buy_volume,
        actual_buy_vwap: buy_vwap,
        actual_buy_fee: buy_fee,
        actual_sell_volume: sell_volume,
        actual_sell_vwap: sell_vwap,
        actual_sell_fee: sell_fee,
        realized_pnl,
        outcome,
        event_ts_ns: timing.event_ts_ns,
        data_age_ms: timing.data_age_ms,
        channel_delay_ms: timing.channel_delay_ms,
    }));
}

/// Momentum round trip on one pair: LIMIT IOC buy at the evaluation-time ask,
/// hold for the sampled duration, then market sell the filled volume.
async fn make_momentum_trade(
    write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    token: &str,
    order: &MomentumOrder,
    filled_volume_rx: &mut mpsc::UnboundedReceiver<OwnTradeFill>,
    timing: ExecTiming,
) {
    let userref = new_userref();
    let vol_formatted = format!("{:.*}", order.volume_decimals_coin, order.volume_coin);
    let limit_price_str = format!("{:.*}", order.price_decimals, order.limit_buy_price);

    let buy_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "limit",
        "price": limit_price_str,
        "volume": vol_formatted,
        "pair": order.pair_name,
        "userref": userref.to_string(),
        "timeinforce": "IOC"
    })
    .to_string();

    if let Err(e) = write.send(Message::Text(buy_msg)).await {
        log::error!(
            "Failed to send momentum LIMIT IOC buy for {}: {:?}",
            order.pair_name,
            e
        );
        log_momentum_outcome(order, userref, &[], &[], "buy_send_failed", timing);
        return;
    }

    log::debug!(
        "Sent momentum LIMIT IOC buy for {} with userref {} at {} (hold {}ms)",
        order.pair_name,
        userref,
        limit_price_str,
        order.hold_ms
    );

    let (buy_fills, channel_closed) =
        collect_fills(filled_volume_rx, userref, "buy", Duration::from_secs(1)).await;
    if channel_closed {
        log::error!(
            "ownTrades channel closed before momentum fill for userref {}",
            userref
        );
        log_momentum_outcome(order, userref, &buy_fills, &[], "buy_channel_closed", timing);
        return;
    }
    let (actual_buy_volume, _, _, _) = summarize_fills(&buy_fills);
    if actual_buy_volume <= 0.0 {
        log::warn!(
            "Momentum buy for {} got no fill (userref {})",
            order.pair_name,
            userref
        );
        log_momentum_outcome(order, userref, &buy_fills, &[], "buy_timeout", timing);
        return;
    }

    // The experiment variable: hold, then exit unconditionally at market.
    tokio::time::sleep(Duration::from_secs_f64(order.hold_ms / 1000.0)).await;

    let filled_vol_formatted = format!("{:.*}", order.volume_decimals_coin, actual_buy_volume);
    let sell_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "sell",
        "ordertype": "market",
        "volume": filled_vol_formatted,
        "pair": order.pair_name,
        "userref": userref.to_string(),
    })
    .to_string();

    if let Err(e) = write.send(Message::Text(sell_msg)).await {
        log::error!(
            "Failed to send momentum market sell for {}: {:?}",
            order.pair_name,
            e
        );
        log_momentum_outcome(order, userref, &buy_fills, &[], "sell_failed", timing);
        return;
    }

    let (sell_fills, _) =
        collect_fills(filled_volume_rx, userref, "sell", Duration::from_millis(500)).await;
    let (actual_sell_volume, _, _, _) = summarize_fills(&sell_fills);

    let outcome = if actual_sell_volume <= 0.0 {
        "sell_failed"
    } else if actual_buy_volume + 1e-12 < order.volume_coin {
        "partial_buy"
    } else {
        "filled"
    };
    log_momentum_outcome(order, userref, &buy_fills, &sell_fills, outcome, timing);

    wait_approx_ms(500).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realized_pnl_converts_sell_proceeds_to_pair1_quote() {
        // Buy 1 @ 100 USD (+1 fee); sell 1 @ 90 EUR (-0.5 fee); FX EUR→USD = 1.2
        let pnl = realized_pnl_pair1_quote(100.0, 1.0, 1.0, 90.0, 1.0, 0.5, 1.2);
        // proceeds = (90 - 0.5) * 1.2 = 107.4; cost = 101; pnl = 6.4
        assert!((pnl - 6.4).abs() < 1e-9);

        let naive = (90.0 * 1.0 - 0.5) - (100.0 * 1.0 + 1.0);
        assert!((pnl - naive).abs() > 1.0);
    }

    #[test]
    fn reject_classification_matches_kraken_errors() {
        assert!(matches!(
            classify_reject("EAccount:Invalid permissions:LMWR trading restricted for US:NJ."),
            RejectClass::Permanent
        ));
        assert!(matches!(
            classify_reject("EService:Market in cancel_only mode"),
            RejectClass::Permanent
        ));
        assert!(matches!(
            classify_reject("Exceeded msg rate"),
            RejectClass::RateLimited
        ));
        assert!(matches!(
            classify_reject("EOrder:Insufficient funds"),
            RejectClass::Cooldown(d) if d == COOLDOWN_INSUFFICIENT_FUNDS
        ));
        assert!(matches!(
            classify_reject("EOrder:Post only order"),
            RejectClass::Cooldown(d) if d == COOLDOWN_DEFAULT
        ));
    }

    #[test]
    fn open_bid_notional_sums_bids_excluding_replaced_pair() {
        let mut state: FxHashMap<&'static str, MakerPairState> = FxHashMap::default();
        state.insert(
            "A/USD",
            MakerPairState {
                bid: Some(RestingQuote {
                    userref: 1,
                    price: 2.0,
                    volume: 5.0,
                }),
                ask: Some(RestingQuote {
                    userref: 2,
                    price: 3.0,
                    volume: 100.0, // asks never count against the bid budget
                }),
                cooldown_until_ns: 0,
            },
        );
        state.insert(
            "B/USD",
            MakerPairState {
                bid: Some(RestingQuote {
                    userref: 3,
                    price: 4.0,
                    volume: 2.5,
                }),
                ask: None,
                cooldown_until_ns: 0,
            },
        );

        assert!((open_bid_notional(&state, "none") - 20.0).abs() < 1e-9);
        // A/USD's bid is being replaced, so only B/USD's counts.
        assert!((open_bid_notional(&state, "A/USD") - 10.0).abs() < 1e-9);
    }
}
