use crate::influx::{log_trade_interval, log_trade_message_receive_speed};
use crate::structs::OrderInfo;
use crate::utils::wait_approx_ms;
use crate::TRADER_BUSY;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
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
    // Set up private WebSocket connection
    // If setup fails, panic since trading cannot proceed without a connection
    let (mut write, _read) = match setup_private_websocket(&token, &private_ws_url).await {
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

    while let Some(order) = trade_rx.recv().await {
        if allow_trades {
            // Log trade message receive speed
            let receive_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            log_trade_message_receive_speed(order.send_timestamp, receive_timestamp);

            // Mark trader as busy before processing
            TRADER_BUSY.store(true, Ordering::Relaxed);

            log::debug!("Sending order starting with {}", order.pair1_name);
            make_trades(&mut write, &token, &order).await;

            // Mark trader as idle after processing
            TRADER_BUSY.store(false, Ordering::Relaxed);
        }
    }
    log::info!("Trading channel closed, exiting trading thread");
}

/// Fire-and-forget trade implementation, with a short wait time between each trade to ensure order.
async fn make_trades(
    write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    token: &str,
    order: &OrderInfo,
) {
    let vol_coin_formatted = format!("{:.*}", order.volume_decimals_coin, order.volume_coin);
    let trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "market",
        "volume": vol_coin_formatted,
        "pair": order.pair1_name,
    })
    .to_string();

    // Trade 1: Buy pair1
    if let Err(e) = write.send(Message::Text(trade_msg)).await {
        log::error!("Failed to send buy order for {}: {:?}", order.pair1_name, e);
        return;
    }

    let t1_0 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    wait_approx_ms(1).await;
    let t1_1 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    // Trade 2: Sell pair2
    let trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "sell",
        "ordertype": "market",
        "volume": vol_coin_formatted,
        "pair": order.pair2_name,
    })
    .to_string();

    if let Err(e) = write.send(Message::Text(trade_msg)).await {
        log::error!(
            "Failed to send sell order for {}: {:?}",
            order.pair2_name,
            e
        );
        return;
    }

    // Log interval between trade 1-2
    let interval_1_2_ms = (t1_1 - t1_0) * 1000.0;
    log_trade_interval("1-2", interval_1_2_ms);

    // Wait 50ms b/c the stablecoin price shouldn't slip. Ensures the prior order has been filled.
    wait_approx_ms(50).await;

    // Trade 3: Buy pair2_stable
    let vol_stable_formatted = format!("{:.*}", order.volume_decimals_stable, order.volume_stable);
    let trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "market",
        "volume": vol_stable_formatted,
        "pair": order.pair2_stable_name,
    })
    .to_string();

    if let Err(e) = write.send(Message::Text(trade_msg)).await {
        log::error!(
            "Failed to send buy order for {}: {:?}",
            order.pair2_stable_name,
            e
        );
        return;
    }

    // Wait 50ms b/c the stablecoin price shouldn't slip. Ensures the prior order has been filled.
    wait_approx_ms(50).await;

    // Trade 4: Sell pair1_stable
    let trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "sell",
        "ordertype": "market",
        "volume": vol_stable_formatted,
        "pair": order.pair1_stable_name,
    })
    .to_string();

    if let Err(e) = write.send(Message::Text(trade_msg)).await {
        log::error!(
            "Failed to send sell order for {}: {:?}",
            order.pair1_stable_name,
            e
        );
        return;
    }
    log::debug!(
        "Successfully completed all 4 trades for arbitrage starting with {}",
        order.pair1_name
    );
}
