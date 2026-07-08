//! Live Kraken book-10 checksum validator.
//!
//! Usage:
//!   cargo run --release --bin book_checksum_probe -- XBT/USD ETH/USD
//!   cargo run --release --bin book_checksum_probe -- --seconds 30 XBT/USD
//!   cargo run --release --bin book_checksum_probe -- --colocated XBT/USD
//!
//! Exits 0 when all checksums match, 1 on any mismatch.

#![allow(dead_code)]

#[path = "../structs.rs"]
mod structs;
#[path = "../orderbook.rs"]
mod orderbook;

use orderbook::OrderBook;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::env;
use std::time::{Duration, Instant};

const DEFAULT_SECONDS: u64 = 15;
const DEFAULT_WS_URL: &str = "wss://ws.kraken.com";
const COLOCATED_WS_URL: &str = "wss://colo-london.vip-ws.kraken.com";

struct PairState {
    book: OrderBook,
    checked: u64,
    mismatches: u64,
    updates: u64,
    snapshots: u64,
}

impl PairState {
    fn new() -> Self {
        Self {
            book: OrderBook::new(),
            checked: 0,
            mismatches: 0,
            updates: 0,
            snapshots: 0,
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut seconds = DEFAULT_SECONDS;
    let mut ws_url = DEFAULT_WS_URL;
    let mut pairs: Vec<String> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--seconds" => {
                i += 1;
                seconds = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_SECONDS);
            }
            "--colocated" => ws_url = COLOCATED_WS_URL,
            flag if flag.starts_with("--") => {
                eprintln!("Unknown flag: {flag}");
                std::process::exit(2);
            }
            pair => pairs.push(pair.to_string()),
        }
        i += 1;
    }

    if pairs.is_empty() {
        pairs = vec!["XBT/USD".to_string(), "ETH/USD".to_string()];
    }

    eprintln!(
        "book_checksum_probe: pairs={:?} seconds={seconds} url={ws_url}",
        pairs
    );

    let url = url::Url::parse(ws_url).expect("invalid websocket url");
    let (ws_stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("websocket connect failed");
    let (mut write, mut read) = ws_stream.split();

    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {"name": "book", "depth": 10},
        "pair": pairs,
    });
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(sub_msg.to_string()))
        .await
        .expect("subscribe send failed");

    let mut state: Vec<(String, PairState)> = pairs
        .iter()
        .map(|p| (p.clone(), PairState::new()))
        .collect();

    let deadline = Instant::now() + Duration::from_secs(seconds);

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = match tokio::time::timeout(remaining, read.next()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => break,
            Err(_) => break,
        };

        let text = match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
            Ok(_) => continue,
            Err(e) => {
                eprintln!("websocket error: {e}");
                break;
            }
        };

        let data: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if data.get("event").is_some() {
            continue;
        }

        let Some(array) = data.as_array() else {
            continue;
        };
        if array.len() < 4 {
            continue;
        }

        let pair = match array[array.len() - 1].as_str() {
            Some(p) => p.to_string(),
            None => continue,
        };
        let Some(pair_state) = state.iter_mut().find(|(p, _)| p == &pair).map(|(_, s)| s) else {
            continue;
        };

        let merged = merge_book_data(array);
        let is_snapshot = merged.get("as").is_some() || merged.get("bs").is_some();

        if is_snapshot {
            let asks = parse_levels(merged.get("as"));
            let bids = parse_levels(merged.get("bs"));
            let ask_refs: Vec<(&str, &str, f64)> = asks
                .iter()
                .map(|(p, v, t)| (p.as_str(), v.as_str(), *t))
                .collect();
            let bid_refs: Vec<(&str, &str, f64)> = bids
                .iter()
                .map(|(p, v, t)| (p.as_str(), v.as_str(), *t))
                .collect();
            pair_state.book.apply_snapshot(&ask_refs, &bid_refs);
            pair_state.snapshots += 1;
            continue;
        }

        if !pair_state.book.ready {
            continue;
        }

        let ask_updates = parse_levels(merged.get("a"));
        let bid_updates = parse_levels(merged.get("b"));
        let ask_refs: Vec<(&str, &str, f64)> = ask_updates
            .iter()
            .map(|(p, v, t)| (p.as_str(), v.as_str(), *t))
            .collect();
        let bid_refs: Vec<(&str, &str, f64)> = bid_updates
            .iter()
            .map(|(p, v, t)| (p.as_str(), v.as_str(), *t))
            .collect();
        pair_state.book.apply_updates(&ask_refs, &bid_refs);
        pair_state.updates += 1;

        if let Some(expected_str) = merged.get("c").and_then(|v| v.as_str()) {
            let Ok(expected) = expected_str.parse::<u32>() else {
                continue;
            };
            pair_state.checked += 1;
            let computed = pair_state.book.compute_checksum();
            if computed != expected {
                pair_state.mismatches += 1;
                eprintln!(
                    "MISMATCH {pair}: expected={expected} computed={computed} msg={text}"
                );
            }
        }
    }

    let mut total_checked = 0u64;
    let mut total_mismatches = 0u64;
    let mut total_updates = 0u64;
    let mut total_snapshots = 0u64;

    eprintln!("--- summary ({seconds}s) ---");
    for pair in &pairs {
        let Some((_, s)) = state.iter().find(|(p, _)| p == pair) else {
            continue;
        };
        eprintln!(
            "{pair}: snapshots={} updates={} checked={} mismatches={}",
            s.snapshots, s.updates, s.checked, s.mismatches
        );
        total_checked += s.checked;
        total_mismatches += s.mismatches;
        total_updates += s.updates;
        total_snapshots += s.snapshots;
    }
    eprintln!(
        "TOTAL: snapshots={total_snapshots} updates={total_updates} checked={total_checked} mismatches={total_mismatches}"
    );

    if total_mismatches > 0 {
        std::process::exit(1);
    }
}

fn merge_book_data(array: &[Value]) -> Value {
    if array.len() == 4 {
        return array[1].clone();
    }
    if array.len() == 5 {
        let mut merged = serde_json::Map::new();
        for obj in [&array[1], &array[2]] {
            if let Some(map) = obj.as_object() {
                for (k, v) in map {
                    merged.insert(k.clone(), v.clone());
                }
            }
        }
        return Value::Object(merged);
    }
    Value::Null
}

fn parse_levels(val: Option<&Value>) -> Vec<(String, String, f64)> {
    let mut out = Vec::new();
    let Some(arr) = val.and_then(|v| v.as_array()) else {
        return out;
    };
    for level in arr {
        let Some(inner) = level.as_array() else {
            continue;
        };
        let Some(price) = inner.first().and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(volume) = inner.get(1).and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(ts) = inner.get(2).and_then(|v| v.as_str()).and_then(|s| s.parse().ok())
        else {
            continue;
        };
        out.push((price.to_string(), volume.to_string(), ts));
    }
    out
}
