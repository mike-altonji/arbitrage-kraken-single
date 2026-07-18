use crate::arb_forensics::{
    try_log, ArbExecutionEvent, FillRecord, ForensicsEvent, MomentumExecutionEvent,
};
use crate::influx::{log_momentum_execution, log_trade_message_receive_speed};
use crate::structs::{MomentumOrder, OrderInfo, TradeCommand};
use crate::utils::wait_approx_ms;
use crate::TRADER_BUSY;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

#[derive(Clone, Debug)]
struct OwnTradeFill {
    userref: i32,
    volume: f64,
    price: f64,
    fee: f64,
    cost: f64,
    side: &'static str,
    time: f64,
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

    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {
            "name": "ownTrades",
            "token": token
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
    let (mut write, mut filled_volume_rx) = if allow_trades {
        let (write, read) = match setup_private_websocket(&token, &private_ws_url).await {
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

        let filled_volume_rx = {
            let (tx, rx) = mpsc::unbounded_channel::<OwnTradeFill>();
            tokio::spawn(async move {
                listen_to_own_trades(read, tx).await;
            });
            rx
        };

        (Some(write), Some(filled_volume_rx))
    } else {
        (None, None)
    };

    while let Some(command) = trade_rx.recv().await {
        TRADER_BUSY.store(true, Ordering::Relaxed);

        let (send_timestamp, updated_pair_kraken_ts) = match &command {
            TradeCommand::Arb(order) => (order.send_timestamp, order.updated_pair_kraken_ts),
            TradeCommand::Momentum(order) => (order.send_timestamp, order.updated_pair_kraken_ts),
        };
        let receive_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        log_trade_message_receive_speed(send_timestamp, receive_timestamp);

        if let (Some(ref mut write), Some(ref mut filled_volume_rx)) =
            (&mut write, &mut filled_volume_rx)
        {
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
                        make_trades_limit_ioc(
                            write,
                            &token,
                            &order,
                            filled_volume_rx,
                            timing,
                        )
                        .await;
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
                        make_momentum_trade(write, &token, &order, filled_volume_rx, timing)
                            .await;
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

        TRADER_BUSY.store(false, Ordering::Relaxed);
    }
    log::info!("Trading channel closed, exiting trading thread");
}

async fn listen_to_own_trades(
    mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    filled_volume_tx: mpsc::UnboundedSender<OwnTradeFill>,
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
                                                        "Received ownTrade fill: userref={}, volume={}, price={}, side={}",
                                                        fill.userref,
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
                    }
                }
            }
            Ok(_) => {
                log::warn!("ownTrades websocket closed or received non-text message");
                break;
            }
            Err(e) => {
                log::error!("Error reading ownTrades websocket: {:?}", e);
                break;
            }
        }
    }
    log::info!("ownTrades listener task ended");
}

fn parse_own_trade_fill(
    trade_info: &serde_json::Map<String, serde_json::Value>,
) -> Option<OwnTradeFill> {
    let userref = trade_info.get("userref").and_then(|v| {
        v.as_i64()
            .map(|i| i as i32)
            .or_else(|| v.as_str().and_then(|s| s.parse::<i32>().ok()))
    })?;

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
    let userref = rand::random::<u32>() as i32;
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
    let userref = rand::random::<u32>() as i32;
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
}
