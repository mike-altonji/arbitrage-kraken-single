use core::sync::atomic::Ordering;
use csv::ReaderBuilder;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use influx_db_client::{Client, Point, Precision, Value};
use reqwest;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::influx::setup_influx;
use crate::telegram::send_telegram_message;

#[derive(Clone)]
pub struct PairToAssets {
    pub base: String,
    pub quote: String,
}

#[derive(Clone)]
pub struct AssetsToPair {
    pub base: String,
    pub quote: String,
    pub pair: String,
}

#[derive(Clone)]
pub struct Spread {
    pub bid: f64,
    pub ask: f64,
    pub kraken_ts: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
}

pub async fn asset_pairs_to_pull(
    fname: &str,
) -> Result<
    (
        HashMap<String, PairToAssets>,
        HashMap<(String, String), AssetsToPair>,
        HashMap<String, Vec<Vec<f64>>>,
    ),
    Box<dyn std::error::Error>,
> {
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
    let assets_url = "https://api.kraken.com/0/public/Assets";
    let resp = reqwest::get(assets_url).await?;
    let text = resp.text().await?;
    let data_assets: serde_json::Value = serde_json::from_str(&text)?;
    let mut pair_to_fee = HashMap::new();

    // Create the {pair: (base, quote)} HashMap
    let mut pair_to_assets = HashMap::new();
    let pairs = data_asset_pairs["result"].as_object().ok_or("No pairs")?;
    for (_pair, details) in pairs {
        let status = details["status"].as_str().unwrap_or("").to_string();
        let pair_ws = details["wsname"].as_str().unwrap_or("").to_string();
        let base = details["base"].as_str().unwrap_or("").to_string();
        let quote = details["quote"].as_str().unwrap_or("").to_string();
        let fee_schedule = details["fees"]
            .as_array()
            .ok_or("Fees not an array")?
            .iter()
            .map(|fee| {
                fee.as_array().ok_or("Fee not an array").and_then(|fee| {
                    let volume_level = fee[0].as_f64().ok_or("Volume level not a float")?;
                    let fee_percentage = fee[1].as_f64().ok_or("Fee percentage not a float")?;
                    Ok(vec![volume_level, fee_percentage])
                })
            })
            .map(|res| res.map_err(|e| e.into())) // Map the error type
            .collect::<Result<Vec<Vec<f64>>, Box<dyn std::error::Error>>>()?;
        pair_to_fee.insert(pair_ws.clone(), fee_schedule);

        // Convert base/quote to ws_name format
        let base_ws = data_assets["result"][&base]["altname"].as_str();
        let quote_ws = data_assets["result"][&quote]["altname"].as_str();

        // Only insert pair: (base, quote) if their values are not missing
        match (base_ws, quote_ws) {
            (Some(base_ws), Some(quote_ws)) => {
                if input_asset_pairs.contains(&pair_ws) && status == "online" {
                    pair_to_assets.insert(
                        pair_ws.to_string(),
                        PairToAssets {
                            base: base_ws.to_string(),
                            quote: quote_ws.to_string(),
                        },
                    );
                }
            }
            _ => {
                log::warn!("Altname does not exist for base {base} or quote {quote}");
            }
        }
    }

    // Create the {(asset, asset): (base, quote, pair)} HashMap
    let mut assets_to_pair = HashMap::new();
    for (pair, assets) in pair_to_assets.clone() {
        let base = assets.base;
        let quote = assets.quote;
        assets_to_pair.insert(
            (base.clone(), quote.clone()),
            AssetsToPair {
                base: base.clone(),
                quote: quote.clone(),
                pair: pair.clone(),
            },
        );
        assets_to_pair.insert(
            (quote.clone(), base.clone()),
            AssetsToPair {
                base: base.clone(),
                quote: quote.clone(),
                pair: pair.clone(),
            },
        );
    }

    Ok((pair_to_assets, assets_to_pair, pair_to_fee))
}

pub fn update_fees_based_on_volume(
    fees: &mut HashMap<String, f64>,
    schedules: &HashMap<String, Vec<Vec<f64>>>,
    vol_30day: f64,
) {
    for (key, schedule) in schedules.iter() {
        let mut fee = 0.0026; // default fee
        for pair in schedule {
            if vol_30day < pair[0] {
                break;
            }
            fee = pair[1] / 100.; // Formatted in pct in Kraken
        }
        fees.insert(key.clone(), fee);
    }
}

pub async fn fetch_spreads(
    all_pairs: HashSet<String>,
    pair_to_spread_vec: Vec<Arc<Mutex<HashMap<String, Spread>>>>,
    pair_to_assets_vec: Vec<HashMap<String, PairToAssets>>,
    pair_status: Arc<Mutex<HashMap<String, bool>>>,
    public_online: Arc<Mutex<bool>>,
) -> Result<(), Box<dyn std::error::Error>> {
    const SLEEP_DURATION: Duration = Duration::from_secs(5);
    loop {
        let (client, retention_policy, batch_size, mut points) = setup_influx().await;
        let (_write, mut read) = setup_websocket(&all_pairs).await;
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    handle_message_text(
                        &text,
                        &public_online,
                        &pair_status,
                        &pair_to_spread_vec,
                        &pair_to_assets_vec,
                        &client,
                        &retention_policy,
                        batch_size,
                        &mut points,
                    )
                    .await?;
                }
                Ok(_) => {
                    let msg = "Websocket connection closed or stopped sending data";
                    log::error!("{}", msg);
                    send_telegram_message(msg).await;
                    tokio::time::sleep(SLEEP_DURATION).await;
                    continue;
                }
                Err(e) => {
                    let msg = format!("Error during websocket communication: {:?}", e);
                    log::error!("{}", msg);
                    send_telegram_message(&msg).await;
                    tokio::time::sleep(SLEEP_DURATION).await;
                    continue;
                }
            }
        }
    }
}

async fn setup_websocket(
    all_pairs: &HashSet<String>,
) -> (
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) {
    let url = url::Url::parse("wss://ws.kraken.com").expect("Public ws unparseable");
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to public websocket");
    let (mut write, read) = ws_stream.split();
    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {"name": "spread"},
        "pair": all_pairs.clone().into_iter().collect::<Vec<String>>(),
    });
    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .expect("Failed to send message");
    log::info!(
        "Subscribed to asset pairs: {:?}",
        all_pairs.iter().collect::<Vec<&String>>()
    );

    (write, read)
}

async fn handle_message_text(
    text: &str,
    public_online: &Arc<Mutex<bool>>,
    pair_status: &Arc<Mutex<HashMap<String, bool>>>,
    pair_to_spread_vec: &Vec<Arc<Mutex<HashMap<String, Spread>>>>,
    pair_to_assets_vec: &Vec<HashMap<String, PairToAssets>>,
    client: &Arc<Client>,
    retention_policy: &Arc<String>,
    batch_size: usize,
    points: &mut Vec<Point>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data: serde_json::Value = serde_json::from_str(text)?;
    if let Some(event) = data["event"].as_str() {
        handle_event(event, &data, public_online, pair_status);
    } else if let Some(array) = data.as_array() {
        handle_array(
            array,
            pair_to_spread_vec,
            pair_to_assets_vec,
            client,
            retention_policy,
            batch_size,
            points,
        )
        .await?;
    }
    Ok(())
}

fn handle_event(
    event: &str,
    data: &serde_json::Value,
    public_online: &Arc<Mutex<bool>>,
    pair_status: &Arc<Mutex<HashMap<String, bool>>>,
) {
    match event {
        "systemStatus" => {
            let status = data["status"].as_str().unwrap_or("") == "online";
            match public_online.lock() {
                Ok(mut public_online_lock) => *public_online_lock = status,
                Err(e) => log::error!("Failed to acquire lock: {:?}", e),
            }
        }
        "subscriptionStatus" => {
            let pair = data["pair"].as_str().unwrap_or("").to_string();
            let status = data["status"].as_str().unwrap_or("") == "subscribed";
            match pair_status.lock() {
                Ok(mut pair_status_lock) => {
                    pair_status_lock.insert(pair, status);
                }
                Err(e) => {
                    log::error!("Failed to acquire lock: {:?}", e);
                }
            };
        }
        _ => {}
    }
}

async fn handle_array(
    array: &Vec<serde_json::Value>,
    pair_to_spread_vec: &Vec<Arc<Mutex<HashMap<String, Spread>>>>,
    pair_to_assets_vec: &Vec<HashMap<String, PairToAssets>>,
    client: &Arc<Client>,
    retention_policy: &Arc<String>,
    batch_size: usize,
    points: &mut Vec<Point>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Spread messages
    if array.len() >= 4 {
        let pair = array[3].as_str().unwrap_or_default().to_string();
        if let Some(inner_array) = array[1].as_array() {
            let bid = get_f64_from_array(&inner_array, 0, "bid")?;
            let ask = get_f64_from_array(&inner_array, 1, "ask")?;
            let kraken_ts = get_f64_from_array(&inner_array, 2, "kraken_ts")?;
            let bid_volume = get_f64_from_array(&inner_array, 3, "bid_volume")?;
            let ask_volume = get_f64_from_array(&inner_array, 4, "ask_volume")?;
            for i in 0..pair_to_assets_vec.len() {
                if pair_to_assets_vec[i].contains_key(&pair.to_string()) {
                    handle_pair(
                        &pair,
                        bid,
                        ask,
                        kraken_ts,
                        bid_volume,
                        ask_volume,
                        &pair_to_spread_vec[i],
                        client,
                        retention_policy,
                        batch_size,
                        points,
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}

async fn handle_pair(
    pair: &str,
    bid: f64,
    ask: f64,
    kraken_ts: f64,
    bid_volume: f64,
    ask_volume: f64,
    pair_to_spread: &Arc<Mutex<HashMap<String, Spread>>>,
    client: &Arc<Client>,
    retention_policy: &Arc<String>,
    batch_size: usize,
    points: &mut Vec<Point>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut locked_pairs = pair_to_spread.lock().unwrap();
    let existing_kraken_ts = locked_pairs
        .get(pair)
        .map(|spread| spread.kraken_ts)
        .unwrap_or(0.0);
    if kraken_ts > existing_kraken_ts {
        // If data is new, update graph and write to DB (we often get old data from Kraken)
        locked_pairs.insert(
            pair.to_string(),
            Spread {
                bid,
                ask,
                kraken_ts,
                bid_volume,
                ask_volume,
            },
        );
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let client = Arc::clone(client);
        let retention_policy_clone = Arc::clone(retention_policy);
        update_points_vector(points, pair.clone(), kraken_ts, now);
        if points.len() >= batch_size {
            let points_clone = points.clone();
            points.clear();
            tokio::spawn(async move {
                spread_latency_to_influx(client, &*retention_policy_clone, points_clone).await;
            });
        }
    }
    Ok(())
}

fn get_f64_from_array(
    array: &[serde_json::Value],
    index: usize,
    name: &str,
) -> Result<f64, Box<dyn std::error::Error>> {
    array
        .get(index)
        .ok_or(format!("Failed to get '{}' from fetched data", name))?
        .as_str()
        .ok_or(format!("Failed to parse '{}' as string", name))?
        .parse::<f64>()
        .map_err(|_| format!("Failed to parse '{}' as f64", name).into())
}

fn update_points_vector(points: &mut Vec<Point>, pair: &str, kraken_ts: f64, update_graph_ts: f64) {
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
        let result = Runtime::new()
            .unwrap()
            .block_on(asset_pairs_to_pull("resources/asset_pairs_a1.csv"));
        assert!(result.is_ok());
        let (pair_to_assets, assets_to_pair, pair_to_fee) = result.unwrap();
        assert!(pair_to_assets.contains_key("EUR/USD"));
        assert_eq!(pair_to_assets["EUR/USD"].base, "EUR");
        assert_eq!(pair_to_assets["EUR/USD"].quote, "USD");
        assert_eq!(pair_to_fee["EUR/USD"][0], vec![0.0, 0.20]);

        let usd_eur = ("USD".to_string(), "EUR".to_string());
        let eur_usd = ("EUR".to_string(), "USD".to_string());
        assert!(assets_to_pair.contains_key(&usd_eur));
        assert!(assets_to_pair.contains_key(&eur_usd));
        assert_eq!(assets_to_pair[&eur_usd].base, "EUR");
        assert_eq!(assets_to_pair[&usd_eur].base, "EUR");
        assert_eq!(assets_to_pair[&eur_usd].quote, "USD");
        assert_eq!(assets_to_pair[&usd_eur].quote, "USD");
        assert_eq!(assets_to_pair[&eur_usd].pair, "EUR/USD");
        assert_eq!(assets_to_pair[&usd_eur].pair, "EUR/USD");
    }

    #[test]
    fn test_update_fees_based_on_volume() {
        let mut fees = HashMap::new();
        fees.insert("BTC".to_string(), 0.0);
        fees.insert("ETH".to_string(), 0.0);

        let mut schedules = HashMap::new();
        schedules.insert(
            "ABC/USD".to_string(),
            vec![vec![1000.0, 0.25], vec![2000.0, 0.20]],
        );
        schedules.insert(
            "XYZ/USD".to_string(),
            vec![vec![500.0, 0.30], vec![1500.0, 0.25]],
        );
        schedules.insert(
            "CAT/USD".to_string(),
            vec![vec![1500.0, 0.20], vec![2000.0, 0.10]],
        );
        schedules.insert(
            "DOG/USD".to_string(),
            vec![vec![500.0, 0.35], vec![1000.0, 0.12]],
        );

        let vol_30day = 1200.0;

        update_fees_based_on_volume(&mut fees, &schedules, vol_30day);

        assert_eq!(fees.get("ABC/USD").unwrap(), &0.0025);
        assert_eq!(fees.get("XYZ/USD").unwrap(), &0.0030);
        assert_eq!(fees.get("CAT/USD").unwrap(), &0.0026);
        assert_eq!(fees.get("DOG/USD").unwrap(), &0.0012);
    }
}
