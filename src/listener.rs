use crate::evaluate_arbitrage;
use crate::structs::OrderInfo;
use crate::structs::PairDataVec;
use crate::utils::send_telegram_message;
use evaluate_arbitrage::evaluate_arbitrage;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Main listener function that sets up WebSocket connection and processes messages
pub async fn run_listening_thread(
    asset_index: &phf::Map<&'static str, usize>,
    pair_data_vec: &mut PairDataVec,
    public_online: &mut bool,
    ws_url: &str,
    pair_names: &[&'static str],
    trade_tx: mpsc::Sender<OrderInfo>,
) {
    const SLEEP_DURATION: Duration = Duration::from_secs(5);

    loop {
        // Set up WebSocket connection to listen to a set of pairs
        let pairs: Vec<String> = asset_index.keys().map(|s| s.to_string()).collect();
        let (_write, mut read) = setup_websocket(&pairs, ws_url).await;

        // Process messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let idx =
                        handle_message(&text, pair_data_vec, public_online, asset_index).await;
                    if let Some(idx) = idx {
                        // Only evaluate arbitrage for non-stablecoin pairs and if the websocket is online
                        if idx > 1 && *public_online {
                            evaluate_arbitrage(pair_data_vec, idx, pair_names, trade_tx.clone());
                        }
                    }
                }
                Ok(_) => {
                    let msg = "Websocket connection closed or stopped sending data";
                    log::warn!("{}", msg);
                    send_telegram_message(msg).await;
                    break; // Break inner loop to reconnect
                }
                Err(e) => {
                    let msg = format!("Error during websocket communication: {:?}", e);
                    log::error!("{}", msg);
                    send_telegram_message(&msg).await;
                    break; // Break inner loop to reconnect
                }
            }
        }
        tokio::time::sleep(SLEEP_DURATION).await;
    }
}

/// Set up WebSocket connection and subscribe to spreads
async fn setup_websocket(
    pairs: &[String],
    ws_url: &str,
) -> (
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) {
    let url = url::Url::parse(ws_url).expect("Failed to parse URL");
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to websocket");
    let (mut write, read) = ws_stream.split();

    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {"name": "spread"},
        "pair": pairs,
    });

    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .expect("Failed to send message to begin websocket subscription");

    log::info!("Subscribed to {} asset pairs", pairs.len());

    (write, read)
}

/// Handle incoming WebSocket messages
/// Logs errors and continues processing
async fn handle_message(
    text: &str,
    pair_data_vec: &mut PairDataVec,
    public_online: &mut bool,
    asset_index: &phf::Map<&'static str, usize>,
) -> Option<usize> {
    let data = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(d) => d,
        Err(e) => {
            log::warn!("Failed to parse message: {:?}", e);
            return None;
        }
    };

    // Handle event messages (systemStatus, subscriptionStatus)
    if let Some(event) = data["event"].as_str() {
        handle_event(event, &data, pair_data_vec, public_online, asset_index).await;
        return None;
    }
    // Handle spread data messages (arrays)
    else if let Some(array) = data.as_array() {
        let idx = handle_spread_data(array, pair_data_vec, asset_index).await;
        return idx;
    }
    return None;
}

/// Handle event messages (systemStatus, subscriptionStatus)
async fn handle_event(
    event: &str,
    data: &serde_json::Value,
    pair_data_vec: &mut PairDataVec,
    public_online: &mut bool,
    asset_index: &phf::Map<&'static str, usize>,
) {
    match event {
        "systemStatus" => {
            let status = data["status"].as_str().unwrap_or("") == "online";
            *public_online = status;
            log::debug!("System status updated: online = {}", status);
        }
        "subscriptionStatus" => {
            let pair = data["pair"].as_str().unwrap_or("");
            let status = ["subscribed", "ok"].contains(&data["status"].as_str().unwrap_or(""));
            if let Some(&idx) = asset_index.get(pair) {
                if let Some(pair_data) = pair_data_vec.get_mut(idx) {
                    pair_data.pair_status = status;
                    log::debug!("Pair {} subscription status: {}", pair, status);
                }
            }
        }
        "heartbeat" => {} // Nothing to do here
        _ => {
            log::debug!("Unhandled event: {}", event);
        }
    }
}

/// Handle spread data messages
/// Silently skips invalid messages
/// Message format: [channelID, [bid, ask, timestamp, bidVolume, askVolume], "spread", pair]
async fn handle_spread_data(
    array: &[serde_json::Value],
    pair_data_vec: &mut PairDataVec,
    asset_index: &phf::Map<&'static str, usize>,
) -> Option<usize> {
    if array.len() != 4 {
        return None;
    }

    let pair = match array[3].as_str() {
        Some(p) => p,
        None => return None,
    };

    // Get index for this pair
    let idx = match asset_index.get(pair) {
        Some(&idx) => idx,
        None => {
            // Pair not in our index, skip it silently
            return None;
        }
    };

    let inner_array = match array[1].as_array() {
        Some(arr) => arr,
        None => return None,
    };

    // Parse values, skip message if any fail
    let bid = match get_f64_from_array(inner_array, 0) {
        Some(v) => v,
        None => return None,
    };
    let ask = match get_f64_from_array(inner_array, 1) {
        Some(v) => v,
        None => return None,
    };
    let _kraken_ts = match get_f64_from_array(inner_array, 2) {
        Some(v) => v,
        None => return None,
    };
    let bid_volume = match get_f64_from_array(inner_array, 3) {
        Some(v) => v,
        None => return None,
    };
    let ask_volume = match get_f64_from_array(inner_array, 4) {
        Some(v) => v,
        None => return None,
    };

    // Update the pair data in the vec
    if let Some(pair_data) = pair_data_vec.get_mut(idx) {
        pair_data.bid_price = bid;
        pair_data.ask_price = ask;
        pair_data.bid_volume = bid_volume;
        pair_data.ask_volume = ask_volume;
    }

    return Some(idx);
}

/// Extract f64 value from JSON array at given index
/// Returns None if parsing fails
fn get_f64_from_array(array: &[serde_json::Value], index: usize) -> Option<f64> {
    array.get(index)?.as_str()?.parse::<f64>().ok()
}
