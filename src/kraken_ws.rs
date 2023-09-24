use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::connect_async;
use serde_json::{json, Value};
use std::collections::HashMap;
use futures_util::StreamExt;
use futures_util::sink::SinkExt;

pub async fn asset_pairs_to_pull() -> Result<HashMap<String, (String, String)>, Box<dyn std::error::Error>> {
    // Fetch the list of all asset pairs
    let asset_pairs_url = "https://api.kraken.com/0/public/AssetPairs";
    let resp = reqwest::get(asset_pairs_url).await?;
    let text = resp.text().await?;
    let data: Value = serde_json::from_str(&text)?;
    let mut pair_to_assets = HashMap::new();

    if let Some(pairs) = data["result"].as_object() {
        for (_pair, details) in pairs {
            let wsname: String = details["wsname"].as_str().unwrap_or("").to_string();
            let base = details["base"].as_str().unwrap_or("").to_string();
            let quote = details["quote"].as_str().unwrap_or("").to_string();
            pair_to_assets.insert(wsname, (base, quote));
        }
    }
    Ok(pair_to_assets)
}


pub async fn fetch_kraken_data_ws(pair_to_assets: HashMap<String, (String, String)>) -> Result<HashMap<String, (f64, f64)>, Box<dyn std::error::Error>> {
    let url = url::Url::parse("wss://ws.kraken.com").unwrap();
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    let (mut write, mut read) = ws_stream.split();
    let subscription_message = json!({
        "event": "subscribe",
        "subscription": {"name": "spread"},
        "pair": pair_to_assets.keys().cloned().collect::<Vec<String>>(),
    });
    write.send(Message::Text(subscription_message.to_string())).await?;

    let mut asset_pairs = HashMap::new();
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let data: Value = serde_json::from_str(&text)?;
                if let Some(array) = data.as_array() {
                    if array.len() >= 4 {
                        let pair = array[3].as_str().unwrap_or_default();
                        if let Some(inner_array) = array[1].as_array() {
                            let bid = inner_array.get(0).and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                            let ask = inner_array.get(1).and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                            asset_pairs.insert(pair.to_string(), (bid, ask));
                            println!("Pair: {} | bid={} ask={}", pair, bid, ask);
                        }
                    }
                }
                
            },
            Err(e) => {
                println!("Error during websocket communication: {:?}", e);
            },
            _ => {} // Handle other message types if needed.
        }
    }
    Ok(asset_pairs)
}
