use crate::evaluate_arbitrage;
use crate::influx::{
    log_arbitrage_evaluation_speed, log_kraken_ingestion_latency, log_listener_loop_speed,
};
use crate::structs::OrderInfo;
use crate::structs::PairDataVec;
use crate::utils::send_telegram_message;
use evaluate_arbitrage::evaluate_arbitrage;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
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
    const MAX_SETUP_ATTEMPTS: u32 = 3;
    let mut loop_counter: usize = 0;
    let mut arbitrage_counter: usize = 0;

    loop {
        let mut setup_attempts = 0;
        let pairs: Vec<String> = asset_index.keys().map(|s| s.to_string()).collect();

        // Try to set up websocket connection, retry on failure. Panic after 3 failures.
        let (_write, mut read) = loop {
            match setup_websocket(&pairs, ws_url).await {
                Ok(streams) => break streams,
                Err(e) => {
                    log::error!("Failed to set up websocket connection: {}", e);
                    setup_attempts += 1;
                    if setup_attempts >= MAX_SETUP_ATTEMPTS {
                        let msg = format!(
                            "Failed to set up websocket connection after {} attempts. Exiting.",
                            MAX_SETUP_ATTEMPTS
                        );
                        log::error!("{}", msg);
                        send_telegram_message(&msg).await;
                        panic!("{}", msg);
                    }
                    tokio::time::sleep(SLEEP_DURATION).await;
                    // Continue loop to retry connection
                }
            }
        };

        // Process messages
        while let Some(msg) = read.next().await {
            let loop_start = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            match msg {
                Ok(Message::Text(text)) => {
                    let idx = handle_message(&text, pair_data_vec, public_online, asset_index);
                    if let Some(idx) = idx {
                        // Only evaluate arbitrage for non-stablecoin pairs and if the websocket is online
                        if idx > 1 && *public_online {
                            let arbitrage_ts_start = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_nanos();
                            evaluate_arbitrage(pair_data_vec, idx, pair_names, trade_tx.clone());
                            let arbitrage_ts_end = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_nanos();
                            if arbitrage_counter >= 10_000 {
                                log_arbitrage_evaluation_speed(
                                    arbitrage_ts_start,
                                    arbitrage_ts_end,
                                );
                                arbitrage_counter = 0;
                            }
                            arbitrage_counter += 1;
                        }
                    }
                    let loop_end = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos();
                    if loop_counter >= 10_000 {
                        log_listener_loop_speed(loop_start, loop_end);
                        loop_counter = 0;
                    }
                    loop_counter += 1;
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
        .map_err(|e| format!("Failed to connect to websocket: {}", e))?;

    let (mut write, read) = ws_stream.split();

    // Create subscription message
    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {"name": "spread"},
        "pair": pairs,
    });

    // Send subscription message
    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .map_err(|e| format!("Failed to send subscription message: {}", e))?;

    log::info!("Subscribed to {} asset pairs", pairs.len());

    Ok((write, read))
}

/// Handle incoming WebSocket messages
/// Logs errors and continues processing
fn handle_message(
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
        handle_event(event, &data, pair_data_vec, public_online, asset_index);
        return None;
    }
    // Handle spread data messages (arrays)
    else if let Some(array) = data.as_array() {
        let idx = handle_spread_data(array, pair_data_vec, asset_index);
        if idx.is_none() {
            log::warn!("Failed to handle spread data. Skipping.");
            return None;
        }
        return idx;
    }
    None
}

/// Handle event messages (systemStatus, subscriptionStatus)
fn handle_event(
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
            log::info!("System status updated: online = {}", status);
        }
        "subscriptionStatus" => {
            let pair = data["pair"].as_str().unwrap_or("");
            let status = ["subscribed", "ok"].contains(&data["status"].as_str().unwrap_or(""));
            if let Some(&idx) = asset_index.get(pair) {
                if let Some(pair_data) = pair_data_vec.get_mut(idx) {
                    pair_data.pair_status = status;
                    log::debug!("Pair {} subscription status updated: {}", pair, status);
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
fn handle_spread_data(
    array: &[serde_json::Value],
    pair_data_vec: &mut PairDataVec,
    asset_index: &phf::Map<&'static str, usize>,
) -> Option<usize> {
    if array.len() != 4 {
        return None;
    }

    let pair = array[3].as_str()?;

    // Get index for this pair
    let idx = match asset_index.get(pair) {
        Some(&idx) => idx,
        None => {
            // Pair not in our index, skip it silently
            return None;
        }
    };

    let inner_array = array[1].as_array()?;

    // Parse values, skip message if any fail
    let bid = get_f64_from_array(inner_array, 0)?;
    let ask = get_f64_from_array(inner_array, 1)?;
    let kraken_ts = get_f64_from_array(inner_array, 2)?;
    let bid_volume = get_f64_from_array(inner_array, 3)?;
    let ask_volume = get_f64_from_array(inner_array, 4)?;

    // Log kraken ts to ingestion ts latency
    let ingestion_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    log_kraken_ingestion_latency(pair, kraken_ts, ingestion_ts);

    // Update the pair data in the vec
    if let Some(pair_data) = pair_data_vec.get_mut(idx) {
        pair_data.bid_price = bid;
        pair_data.ask_price = ask;
        pair_data.bid_volume = bid_volume;
        pair_data.ask_volume = ask_volume;
    } else {
        return None;
    }

    Some(idx)
}

/// Extract f64 value from JSON array at given index
/// Returns None if parsing fails
fn get_f64_from_array(array: &[serde_json::Value], index: usize) -> Option<f64> {
    array.get(index)?.as_str()?.parse::<f64>().ok()
}
