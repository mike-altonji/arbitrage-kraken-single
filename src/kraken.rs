use core::sync::atomic::Ordering;
use csv::ReaderBuilder;
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use influx_db_client::{reqwest::Url, Client, Point, Precision, Value};
use reqwest;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

pub async fn asset_pairs_to_pull(
    fname: &str,
) -> Result<HashMap<String, (String, String)>, Box<dyn std::error::Error>> {
    // Define the set of valid bases and quotes
    let mut rdr = ReaderBuilder::new().from_reader(File::open(fname)?);
    let mut input_asset_pairs = HashSet::new();
    for result in rdr.records() {
        let record = result?;
        input_asset_pairs.insert(record[0].to_string());
    }

    // Fetch the list of all asset pairs
    let asset_pairs_url = "https://api.kraken.com/0/public/AssetPairs";
    let resp = reqwest::get(asset_pairs_url).await?;
    let text = resp.text().await?;
    let data_asset_pairs: serde_json::Value = serde_json::from_str(&text)?;

    // Fetch the list of all assets
    let asset_pairs_url = "https://api.kraken.com/0/public/Assets";
    let resp = reqwest::get(asset_pairs_url).await?;
    let text = resp.text().await?;
    let data_assets: serde_json::Value = serde_json::from_str(&text)?;

    let mut pair_to_assets = HashMap::new();
    if let Some(pairs) = data_asset_pairs["result"].as_object() {
        for (_pair, details) in pairs {
            let status = details["status"].as_str().unwrap_or("").to_string();
            let pair_ws = details["wsname"].as_str().unwrap_or("").to_string();
            let base = details["base"].as_str().unwrap_or("").to_string();
            let quote = details["quote"].as_str().unwrap_or("").to_string();

            // Convert base/quote to ws_name format
            let base_ws = data_assets["result"][&base]["altname"].as_str();
            let quote_ws = data_assets["result"][&quote]["altname"].as_str();

            // Only insert pair: (base, quote) if their values are not missing
            match (base_ws, quote_ws) {
                (Some(base_ws), Some(quote_ws)) => {
                    if input_asset_pairs.contains(&pair_ws) && status == "online" {
                        pair_to_assets.insert(
                            pair_ws.to_string(),
                            (base_ws.to_string(), quote_ws.to_string()),
                        );
                    }
                }
                _ => {
                    log::warn!("Altname does not exist for base or quote");
                }
            }
        }
    }

    Ok(pair_to_assets)
}

pub async fn fetch_kraken_data_ws(
    all_pairs: HashSet<String>,
    shared_asset_pairs_vec: Vec<Arc<Mutex<HashMap<String, (f64, f64, f64, f64, f64)>>>>,
    pair_to_assets_vec: Vec<HashMap<String, (String, String)>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = url::Url::parse("wss://ws.kraken.com").unwrap();
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    let (mut write, mut read) = ws_stream.split();
    let subscription_message = serde_json::json!({
        "event": "subscribe",
        "subscription": {"name": "spread"},
        "pair": all_pairs.clone().into_iter().collect::<Vec<String>>(),
    });
    write
        .send(Message::Text(subscription_message.to_string()))
        .await?;
    log::info!(
        "Subscribed to asset pairs: {:?}",
        all_pairs.iter().collect::<Vec<&String>>()
    );

    // Set up InfluxDB client
    dotenv::dotenv().ok();
    let host = std::env::var("INFLUXDB_HOST").expect("INFLUXDB_HOST must be set");
    let port = std::env::var("INFLUXDB_PORT").expect("INFLUXDB_PORT must be set");
    let db_name = std::env::var("DB_NAME").expect("DB_NAME must be set");
    let user = std::env::var("DB_USER").expect("DB_USER must be set");
    let password = std::env::var("DB_PASSWORD").expect("DB_PASSWORD must be set");
    let retention_policy_var = Arc::new(std::env::var("RP_NAME").expect("RP_NAME must be set"));
    let retention_policy_clone = Arc::clone(&retention_policy_var);
    let client = Arc::new(
        Client::new(
            Url::parse(&format!("http://{}:{}", &host, &port)).unwrap(),
            &db_name,
        )
        .set_authentication(&user, &password),
    );

    let batch_size: usize = 500;
    let mut points = Vec::new();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let data: serde_json::Value = serde_json::from_str(&text)?;
                if let Some(array) = data.as_array() {
                    if array.len() >= 4 {
                        let pair = array[3].as_str().unwrap_or_default().to_string();
                        if let Some(inner_array) = array[1].as_array() {
                            let bid = inner_array
                                .get(0)
                                .and_then(|s| s.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap();
                            let ask = inner_array
                                .get(1)
                                .and_then(|s| s.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap();
                            let kraken_ts = inner_array
                                .get(2)
                                .and_then(|s| s.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap();
                            let bid_volume = inner_array
                                .get(3)
                                .and_then(|s| s.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap();
                            let ask_volume = inner_array
                                .get(4)
                                .and_then(|s| s.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap();
                            for i in 0..pair_to_assets_vec.len() {
                                if pair_to_assets_vec[i].contains_key(&pair.to_string()) {
                                    let mut locked_pairs =
                                        shared_asset_pairs_vec[i].lock().unwrap();
                                    let existing_kraken_ts =
                                        match locked_pairs.get(&pair.to_string()) {
                                            Some(&(_, _, ts, _, _)) => ts,
                                            None => 0.0,
                                        };
                                    if kraken_ts > existing_kraken_ts {
                                        // If data is new, update graph and write to DB (we often get old data from Kraken)
                                        locked_pairs.insert(
                                            pair.to_string(),
                                            (bid, ask, kraken_ts, bid_volume, ask_volume),
                                        );
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_secs_f64();
                                        let client = Arc::clone(&client);
                                        let retention_policy = Arc::clone(&retention_policy_clone);
                                        let pair_ = pair.clone();
                                        update_points_vector(&mut points, pair_, kraken_ts, now);
                                        if points.len() >= batch_size {
                                            let points_clone = points.clone();
                                            points.clear();
                                            tokio::spawn(async move {
                                                spread_latency_to_influx(
                                                    client,
                                                    &*retention_policy,
                                                    points_clone,
                                                )
                                                .await;
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(_) => {
                log::error!("Websocket connection closed or stopped sending data");
                return Ok(());
            }
            Err(e) => {
                log::error!("Error during websocket communication: {:?}", e);
                return Err(Box::new(e));
            }
        }
    }
    Ok(())
}

pub async fn execute_trade(
    _asset1: &str,
    _asset2: &str,
    _volume: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    // log::info!("TODO: Buy {} of {} using {}", volume, asset2, asset1);
    Ok(())
}

fn update_points_vector(
    points: &mut Vec<Point>,
    pair: String,
    kraken_ts: f64,
    update_graph_ts: f64,
) {
    let latency = update_graph_ts - kraken_ts;
    let point = Point::new("spread_latency")
        .add_tag("pair", Value::String(pair.to_string()))
        .add_field("kraken_ts", Value::Float(kraken_ts))
        .add_field("update_graph_ts", Value::Float(update_graph_ts))
        .add_field("latency", latency);
    points.push(point);
}

async fn spread_latency_to_influx(client: Arc<Client>, retention_policy: &str, points: Vec<Point>) {
    if let Err(e) = client
        .write_points(points, Some(Precision::Nanoseconds), Some(retention_policy))
        .await
    {
        static ERROR_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let error_count = ERROR_COUNTER.fetch_add(1, Ordering::Relaxed);
        if error_count % 1000 == 0 {
            log::error!("Failed to write to spread_latency: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_asset_pairs_to_pull() {
        let result = Runtime::new().unwrap().block_on(asset_pairs_to_pull(
            "resources/kraken_pairs/asset_pairs_a1.csv",
        ));
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert!(pairs.contains_key("EUR/USD"));
        assert_eq!(pairs["EUR/USD"].0, "EUR");
        assert_eq!(pairs["EUR/USD"].1, "USD");
    }
}
