use crate::evaluate_arbitrage;
use crate::influx::{
    log_arbitrage_evaluation_speed, log_kraken_ingestion_latency, log_listener_loop_speed,
};
use crate::orderbook::OrderBookVec;
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

const CHECKSUM_MISMATCH_THRESHOLD: u32 = 3;

enum BookHandleResult {
    BboChanged(usize),
    ChecksumMismatch,
    None,
}

/// Main listener function that sets up WebSocket connection and processes messages
pub async fn run_listening_thread(
    asset_index: &phf::Map<&'static str, usize>,
    pair_data_vec: &mut PairDataVec,
    order_book_vec: &mut OrderBookVec,
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

        reset_order_books(order_book_vec);

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

        let mut checksum_mismatch_count = 0u32;

        // Process messages
        while let Some(msg) = read.next().await {
            let loop_start = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            match msg {
                Ok(Message::Text(text)) => {
                    let result = handle_message(
                        &text,
                        pair_data_vec,
                        order_book_vec,
                        public_online,
                        asset_index,
                    );
                    match result {
                        BookHandleResult::BboChanged(idx) => {
                            if idx > 1
                                && *public_online
                                && order_book_vec.get(idx).map(|b| b.ready).unwrap_or(false)
                            {
                                let pair = pair_names.get(idx).copied().unwrap_or("");
                                let kraken_ts = pair_data_vec[idx].kraken_ts;
                                let ingestion_ts = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs_f64();
                                log_kraken_ingestion_latency(pair, kraken_ts, ingestion_ts);

                                let arbitrage_ts_start = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos();
                                evaluate_arbitrage(
                                    pair_data_vec,
                                    idx,
                                    pair_names,
                                    trade_tx.clone(),
                                );
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
                        BookHandleResult::ChecksumMismatch => {
                            checksum_mismatch_count += 1;
                            log::error!(
                                "Book checksum mismatch (count={})",
                                checksum_mismatch_count
                            );
                            if checksum_mismatch_count >= CHECKSUM_MISMATCH_THRESHOLD {
                                let msg = format!(
                                    "Book checksum mismatch threshold reached ({}) — reconnecting",
                                    CHECKSUM_MISMATCH_THRESHOLD
                                );
                                log::error!("{}", msg);
                                send_telegram_message(&msg).await;
                                break;
                            }
                        }
                        BookHandleResult::None => {}
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

fn reset_order_books(order_book_vec: &mut OrderBookVec) {
    for book in order_book_vec.iter_mut() {
        book.reset();
    }
}

/// Set up WebSocket connection and subscribe to order book
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
        "subscription": {"name": "book", "depth": 10},
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
    order_book_vec: &mut OrderBookVec,
    public_online: &mut bool,
    asset_index: &phf::Map<&'static str, usize>,
) -> BookHandleResult {
    let data = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(d) => d,
        Err(e) => {
            log::warn!("Failed to parse message: {:?}", e);
            return BookHandleResult::None;
        }
    };

    // Handle event messages (systemStatus, subscriptionStatus)
    if let Some(event) = data["event"].as_str() {
        handle_event(event, &data, pair_data_vec, public_online, asset_index);
        return BookHandleResult::None;
    }

    if let Some(array) = data.as_array() {
        return handle_book_data(array, pair_data_vec, order_book_vec, asset_index);
    }

    BookHandleResult::None
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
        "heartbeat" => {}
        _ => {
            log::debug!("Unhandled event: {}", event);
        }
    }
}

/// Handle book snapshot and update messages.
///
/// Snapshot: `[channelID, {as, bs}, "book-10", pair]`
/// Update (combined): `[channelID, {a/b, c?}, "book-10", pair]`
/// Update (split): `[channelID, {a}, {b, c}, "book-10", pair]`
fn handle_book_data(
    array: &[serde_json::Value],
    pair_data_vec: &mut PairDataVec,
    order_book_vec: &mut OrderBookVec,
    asset_index: &phf::Map<&'static str, usize>,
) -> BookHandleResult {
    if array.len() < 4 {
        return BookHandleResult::None;
    }

    let channel = array[array.len() - 2].as_str().unwrap_or("");
    if !channel.starts_with("book-") {
        return BookHandleResult::None;
    }

    // Get index for this pair
    let pair = match array[array.len() - 1].as_str() {
        Some(p) => p,
        None => return BookHandleResult::None,
    };
    let idx = match asset_index.get(pair) {
        Some(&idx) => idx,
        None => return BookHandleResult::None,
    };

    let book = match order_book_vec.get_mut(idx) {
        Some(b) => b,
        None => return BookHandleResult::None,
    };
    let pair_data = match pair_data_vec.get_mut(idx) {
        Some(p) => p,
        None => return BookHandleResult::None,
    };

    // Snapshot: single data object with `as` / `bs`
    if array.len() == 4 {
        let data = &array[1];
        if data.get("as").is_some() || data.get("bs").is_some() {
            let msg_ts = apply_snapshot(book, data);
            let change = book.sync_bbo(pair_data, msg_ts);
            return if change.changed {
                BookHandleResult::BboChanged(idx)
            } else {
                BookHandleResult::None
            };
        }

        // Update in a single object
        return apply_book_update(book, pair_data, data, idx, pair);
    }

    // Split update: merge `a` from array[1] and `b`/`c` from array[2]
    if array.len() == 5 {
        let mut merged = serde_json::Map::new();
        if let Some(obj) = array[1].as_object() {
            for (k, v) in obj {
                merged.insert(k.clone(), v.clone());
            }
        }
        if let Some(obj) = array[2].as_object() {
            for (k, v) in obj {
                merged.insert(k.clone(), v.clone());
            }
        }
        let data = serde_json::Value::Object(merged);
        return apply_book_update(book, pair_data, &data, idx, pair);
    }

    BookHandleResult::None
}

fn apply_snapshot(book: &mut crate::orderbook::OrderBook, data: &serde_json::Value) -> f64 {
    let mut asks: Vec<(&str, &str, f64)> = Vec::new();
    let mut bids: Vec<(&str, &str, f64)> = Vec::new();

    if let Some(as_arr) = data.get("as").and_then(|v| v.as_array()) {
        for level in as_arr {
            if let Some(parsed) = parse_level(level) {
                asks.push(parsed);
            }
        }
    }
    if let Some(bs_arr) = data.get("bs").and_then(|v| v.as_array()) {
        for level in bs_arr {
            if let Some(parsed) = parse_level(level) {
                bids.push(parsed);
            }
        }
    }

    book.apply_snapshot(&asks, &bids)
}

fn apply_book_update(
    book: &mut crate::orderbook::OrderBook,
    pair_data: &mut crate::structs::PairData,
    data: &serde_json::Value,
    idx: usize,
    pair: &str,
) -> BookHandleResult {
    if !book.ready {
        return BookHandleResult::None;
    }

    let ask_updates = parse_levels_from_value(data.get("a"));
    let bid_updates = parse_levels_from_value(data.get("b"));
    let ask_refs: Vec<(&str, &str, f64)> = ask_updates
        .iter()
        .map(|(p, v, t)| (p.as_str(), v.as_str(), *t))
        .collect();
    let bid_refs: Vec<(&str, &str, f64)> = bid_updates
        .iter()
        .map(|(p, v, t)| (p.as_str(), v.as_str(), *t))
        .collect();
    let msg_ts = book.apply_updates(&ask_refs, &bid_refs);

    if let Some(checksum_str) = data.get("c").and_then(|v| v.as_str()) {
        if let Ok(expected) = checksum_str.parse::<u32>() {
            let computed = book.compute_checksum();
            if computed != expected {
                log::error!(
                    "Checksum mismatch for {} (idx {}): expected {}, computed {}",
                    pair,
                    idx,
                    expected,
                    computed
                );
                book.ready = false;
                pair_data.pair_status = false;
                return BookHandleResult::ChecksumMismatch;
            }
        }
    }

    let change = book.sync_bbo(pair_data, msg_ts);
    if change.changed {
        BookHandleResult::BboChanged(idx)
    } else {
        BookHandleResult::None
    }
}

fn parse_levels_from_value(val: Option<&serde_json::Value>) -> Vec<(String, String, f64)> {
    let mut out = Vec::new();
    if let Some(arr) = val.and_then(|v| v.as_array()) {
        for level in arr {
            if let Some((price, volume, ts)) = parse_level_owned(level) {
                out.push((price, volume, ts));
            }
        }
    }
    out
}

fn parse_level_owned(level: &serde_json::Value) -> Option<(String, String, f64)> {
    let inner = level.as_array()?;
    let price = inner.first()?.as_str()?.to_string();
    let volume = inner.get(1)?.as_str()?.to_string();
    let ts = inner.get(2)?.as_str()?.parse::<f64>().ok()?;
    Some((price, volume, ts))
}

fn parse_level(level: &serde_json::Value) -> Option<(&str, &str, f64)> {
    let inner = level.as_array()?;
    let price = inner.first()?.as_str()?;
    let volume = inner.get(1)?.as_str()?;
    let ts = inner.get(2)?.as_str()?.parse::<f64>().ok()?;
    Some((price, volume, ts))
}
