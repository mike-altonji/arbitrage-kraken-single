use crate::structs::OrderInfo;
use crate::TRADER_BUSY;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Set up private WebSocket connection and subscribe to own trades
async fn setup_private_websocket(
    token: &str,
    ws_url: &str,
) -> (
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) {
    let url = url::Url::parse(ws_url).expect("Failed to parse URL");
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to private websocket");
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
        .expect("Failed to send message to begin websocket subscription");

    log::info!("Subscribed to own trades via private websocket.");

    (write, read)
}

/// Trading thread main loop
/// Receives OrderInfo messages and executes the trading logic
pub async fn run_trading_thread(
    token: String,
    private_ws_url: String,
    mut trade_rx: mpsc::Receiver<OrderInfo>,
    allow_trades: bool,
) {
    log::debug!("Starting trading thread");

    // Set up private WebSocket connection
    let (mut write, _read) = setup_private_websocket(&token, &private_ws_url).await;

    while let Some(order) = trade_rx.recv().await {
        if allow_trades {
            // Mark trader as busy before processing
            TRADER_BUSY.store(true, Ordering::Relaxed);

            log::debug!("Sending order starting with {}", order.pair1_name);
            make_trades(&mut write, &token, &order).await;

            // Mark trader as idle after processing
            TRADER_BUSY.store(false, Ordering::Relaxed);
        } else {
            log::debug!(
                "Trading is disabled, skipping order for {}",
                order.pair1_name
            );
        }
    }
    log::debug!("Trading channel closed, exiting trading thread");
}

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

    if let Err(e) = write.send(Message::Text(trade_msg)).await {
        log::error!("Failed to send buy order for {}: {:?}", order.pair1_name, e);
        return;
    }
    log::debug!("Sent buy order for {}", order.pair1_name);
    tokio::time::sleep(Duration::from_millis(1)).await; // TODO: Make this 1ms in a better way soon

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
    log::debug!("Sent sell order for {}", order.pair2_name);
    tokio::time::sleep(Duration::from_millis(1)).await; // TODO: Make this 1ms in a better way soon

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
    log::debug!("Sent buy order for {}", order.pair2_stable_name);
    tokio::time::sleep(Duration::from_millis(1)).await; // TODO: Make this 1ms in a better way soon

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
    log::debug!("Sent sell order for {}", order.pair1_stable_name);
}
