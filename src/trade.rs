use crate::influx::log_trade_message_receive_speed;
use crate::structs::OrderInfo;
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
    // Parse URL
    let url = url::Url::parse(ws_url)
        .map_err(|e| format!("Failed to parse WebSocket URL '{}': {}", ws_url, e))?;

    // Connect to websocket
    let (ws_stream, _) = connect_async(url)
        .await
        .map_err(|e| format!("Failed to connect to private websocket: {}", e))?;

    let (mut write, read) = ws_stream.split();

    // Create subscription message
    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {
            "name": "ownTrades",
            "token": token
        }
    });

    // Send subscription message
    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .map_err(|e| format!("Failed to send subscription message: {}", e))?;

    log::info!("Subscribed to own trades via private websocket.");

    Ok((write, read))
}

/// Trading thread main loop
/// Receives OrderInfo messages and executes the trading logic
pub async fn run_trading_thread(
    token: String,
    private_ws_url: String,
    mut trade_rx: mpsc::Receiver<OrderInfo>,
    allow_trades: bool,
) {
    // Set up private WebSocket connection and ownTrades listener only if trading
    let (mut write, mut filled_volume_rx) = if allow_trades {
        // If setup fails, panic since trading cannot proceed without a connection
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

        // Listen to ownTrades messages
        let (_filled_volume_tx, filled_volume_rx) = {
            let (tx, rx) = mpsc::unbounded_channel::<(i32, f64)>();
            let token_clone = token.clone();
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                listen_to_own_trades(read, &token_clone, tx_clone).await;
            });
            (tx, rx)
        };

        (Some(write), Some(filled_volume_rx))
    } else {
        (None, None)
    };

    while let Some(order) = trade_rx.recv().await {
        // Mark trader as busy before processing
        TRADER_BUSY.store(true, Ordering::Relaxed);

        // Log trade message receive speed
        let receive_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        log_trade_message_receive_speed(order.send_timestamp, receive_timestamp);

        if let (Some(ref mut write), Some(ref mut filled_volume_rx)) =
            (&mut write, &mut filled_volume_rx)
        {
            log::debug!("Sending order starting with {}", order.pair1_name);
            make_trades_limit_ioc(write, &token, &order, filled_volume_rx).await;
        }

        // Mark trader as idle after processing
        TRADER_BUSY.store(false, Ordering::Relaxed);
    }
    log::info!("Trading channel closed, exiting trading thread");
}

/// Listen to ownTrades WebSocket messages and extract filled volumes by userref
async fn listen_to_own_trades(
    mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    _token: &str,
    filled_volume_tx: mpsc::UnboundedSender<(i32, f64)>,
) {
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                    // ownTrades messages come as arrays: [trades_array, "ownTrades", feed_detail]
                    if let Some(array) = data.as_array() {
                        if array.len() >= 3 {
                            // Check that array[1] is "ownTrades"
                            if array[1].as_str() == Some("ownTrades") {
                                // array[0] is the trades array
                                if let Some(trades_array) = array[0].as_array() {
                                    // Iterate through each trade in the trades array
                                    for trade_obj in trades_array {
                                        if let Some(trade_obj_map) = trade_obj.as_object() {
                                            // Each trade object has trade ID as key, trade data as value
                                            for (_trade_id, trade_data) in trade_obj_map {
                                                if let Some(trade_info) = trade_data.as_object() {
                                                    // Extract userref and volume
                                                    // userref can be integer or string, vol is string
                                                    let userref_opt =
                                                        trade_info.get("userref").and_then(|v| {
                                                            // Try as integer first
                                                            v.as_i64().map(|i| i as i32).or_else(
                                                                || {
                                                                    // Fall back to string parsing
                                                                    v.as_str().and_then(|s| {
                                                                        s.parse::<i32>().ok()
                                                                    })
                                                                },
                                                            )
                                                        });

                                                    let vol_opt = trade_info
                                                        .get("vol")
                                                        .and_then(|v| v.as_str())
                                                        .and_then(|s| s.parse::<f64>().ok());

                                                    if let (Some(userref), Some(volume)) =
                                                        (userref_opt, vol_opt)
                                                    {
                                                        // Only send if it's a buy order (type "buy" or "b")
                                                        let order_type = trade_info
                                                            .get("type")
                                                            .and_then(|v| v.as_str());

                                                        if order_type == Some("buy")
                                                            || order_type == Some("b")
                                                        {
                                                            log::debug!(
                                                                "Received ownTrade fill: userref={}, volume={}, type={:?}",
                                                                userref,
                                                                volume,
                                                                order_type
                                                            );
                                                            if let Err(_) = filled_volume_tx
                                                                .send((userref, volume))
                                                            {
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

/// LIMIT IOC buy order, listen to ownTrades to get filled volume, then market sell
async fn make_trades_limit_ioc(
    write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    token: &str,
    order: &OrderInfo,
    filled_volume_rx: &mut mpsc::UnboundedReceiver<(i32, f64)>,
) {
    let userref = rand::random::<u32>() as i32;
    let vol_coin_formatted = format!("{:.*}", order.volume_decimals_coin, order.volume_coin);
    let limit_price_str = format!("{:.*}", order.price_decimals, order.pair1_price);

    // Trade 1: LIMIT IOC buy order for pair1
    // Note: Kraken WebSocket API uses "timeinforce" parameter for IOC orders
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
        return;
    }

    log::debug!(
        "Sent LIMIT IOC buy order for {} with userref {} and limit price {}",
        order.pair1_name,
        userref,
        limit_price_str
    );

    // Wait for ownTrades message with the filled volume (with timeout)
    let timeout_duration = Duration::from_secs(1);
    let filled_volume = match timeout(timeout_duration, async {
        loop {
            if let Some((ref_userref, volume)) = filled_volume_rx.recv().await {
                if ref_userref == userref {
                    log::debug!("Matched userref {} with volume {}", ref_userref, volume);
                    return Some(volume);
                } else {
                    log::debug!(
                        "Received userref {} but waiting for {}",
                        ref_userref,
                        userref
                    );
                }
            } else {
                return None;
            }
        }
    })
    .await
    {
        Ok(Some(vol)) => vol,
        Ok(None) => {
            log::error!(
                "ownTrades channel closed before receiving fill for userref {}",
                userref
            );
            return;
        }
        Err(_) => {
            log::error!(
                "Timeout waiting for ownTrades fill confirmation for userref {}",
                userref
            );
            return;
        }
    };

    log::debug!(
        "Received filled volume {} for userref {} on {}",
        filled_volume,
        userref,
        order.pair1_name
    );

    // Trade 2: Market sell order for pair2 using the filled volume
    let filled_vol_formatted = format!("{:.*}", order.volume_decimals_coin, filled_volume);
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
        return;
    }

    log::debug!(
        "Successfully completed LIMIT IOC trades for arbitrage starting with {}",
        order.pair1_name
    );

    // Block additional trades for 500ms to avoid race conditions
    wait_approx_ms(500).await;
}
