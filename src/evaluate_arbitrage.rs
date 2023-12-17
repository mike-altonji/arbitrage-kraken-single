use crate::graph_algorithms::bellman_ford_negative_cycle;
use crate::kraken_private::execute_trade;
use crate::structs::{
    AssetsToPair, BaseQuote, OrderMap, PairToAssets, PairToDecimals, PairToTradeMin,
    PairToVolatility, Spread,
};
use crate::structs::{Edge, PairToSpread};
use crate::trade::rotate_path;
use futures_util::SinkExt;
use influx_db_client::{reqwest::Url, Client, Point, Precision, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const MIN_ROI: f64 = 1.00;
const MIN_PROFIT: f64 = 0.01;
const MAX_TRADES: usize = 4;
const MAX_LATENCY: f64 = 0.200;

pub async fn evaluate_arbitrage_opportunities(
    pair_to_assets: PairToAssets,
    assets_to_pair: AssetsToPair,
    pair_to_spread: Arc<Mutex<PairToSpread>>,
    fees: Arc<Mutex<HashMap<String, f64>>>,
    pair_to_decimals: PairToDecimals,
    pair_status: Arc<Mutex<HashMap<String, bool>>>,
    public_online: Arc<Mutex<bool>>,
    p90_latency: Arc<Mutex<f64>>,
    allow_trades: bool,
    token: Option<&str>,
    graph_id: i64,
    volatility: Arc<Mutex<PairToVolatility>>,
    orders: Arc<Mutex<OrderMap>>,
    balances: Arc<Mutex<HashMap<String, f64>>>,
    pair_to_trade_mins: PairToTradeMin,
) -> Result<(), Box<dyn std::error::Error>> {
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
            Url::parse(&format!("http://{}:{}", &host, &port)).expect("Failed to parse URL"),
            &db_name,
        )
        .set_authentication(&user, &password),
    );
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let p90_latency_value = p90_latency.lock().unwrap().clone();
    let assets_to_pair_clone = assets_to_pair.clone();

    let starters = HashSet::from(["USD".to_string(), "EUR".to_string()]);

    // Set up websocket connection to private Kraken endpoint if trading & subscribe to `ownTrades` to stay connected
    let mut private_ws = None;
    if allow_trades {
        let (ws, _) = connect_async("wss://ws-auth.kraken.com").await?;
        private_ws = Some(Arc::new(tokio::sync::Mutex::new(ws)));
        let sub_msg = serde_json::json!({
            "event": "subscribe",
            "subscription": {
                "name": "ownTrades",
                "token": token
            }
        });
        if let Some(ws) = &private_ws {
            let mut lock = ws.lock().await;
            lock.send(Message::Text(sub_msg.to_string())).await?;
        }
    }

    // Give pair_to_spread time to populate
    tokio::time::sleep(Duration::from_secs(5)).await;

    let asset_to_index = generate_asset_to_index_map(&pair_to_assets);
    let n = asset_to_index.len();
    log::info!(
        "Asset to index map (Graph {}): {:?}",
        graph_id,
        asset_to_index
    );

    let mut counter = 0;
    loop {
        // If the public endpoint is down, don't attempt evaluation
        if !*public_online.lock().unwrap() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        // Create graph and run Bellman Ford
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        // Take pauses so I don't overheat computer (actually waits longer...eval times went from 1.5us to 1.5ms. Fine with this for now!)
        tokio::time::sleep(Duration::from_micros(10)).await;

        let pair_to_spread = pair_to_spread.lock().unwrap().clone();
        let pair_status_clone = pair_status.lock().unwrap().clone();
        let fees_clone = fees.lock().unwrap().clone();
        let (rate_edges, rate_map, volume_map) = prepare_graph(
            &pair_to_spread,
            &pair_to_assets,
            &asset_to_index,
            &fees_clone,
            &pair_status_clone,
        );
        let path = bellman_ford_negative_cycle(n, &rate_edges, 0);

        // Log a subsample of events every N iterations
        if counter == 1000 {
            let end_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let client_clone = Arc::clone(&client);
            let retention_policy = Arc::clone(&retention_policy_clone);
            tokio::spawn(async move {
                save_evaluation_time_to_influx(
                    client_clone,
                    &*retention_policy,
                    graph_id,
                    start_time,
                    end_time,
                )
                .await;
            });
            counter = 0
        }
        counter += 1;

        // Arbitrage opportunity found
        if let Some(path) = path {
            let mut path_names: Vec<String> = path
                .iter()
                .map(|&i| {
                    asset_to_index
                        .iter()
                        .find(|&(_, &v)| v == i)
                        .unwrap()
                        .0
                        .clone()
                })
                .collect();
            rotate_path(
                &mut path_names,
                &starters,
                &volatility.lock().unwrap().clone(),
                &assets_to_pair_clone,
            );
            let (mut min_volume, end_volume, rates) =
                limiting_volume(&path_names, &rate_map, &volume_map);

            let high_enough_trade_volume = is_trade_volume_high_enough(
                &min_volume,
                &path_names,
                &pair_to_trade_mins,
                assets_to_pair.clone(),
                &pair_to_spread,
            );

            let rates_clone = rates.clone();

            // Log and send message at most every 5 seconds
            let path_names_clone = path_names.clone();
            let retention_policy = Arc::clone(&retention_policy_clone);
            let client1 = Arc::clone(&client);
            let client2 = Arc::clone(&client);
            let sem_clone = Arc::clone(&semaphore);

            // Only record arbitrage opportunity if it's tradeable
            if high_enough_trade_volume {
                tokio::spawn(async move {
                    if let Ok(_permit) = sem_clone.try_acquire() {
                        arbitrage_details_to_influx(
                            client1,
                            graph_id,
                            min_volume,
                            end_volume,
                            path_names[0].to_string(),
                            path_names,
                            rates,
                        )
                        .await;

                        // Log +/- 5 minutes of raw data. Don't need to wait for it to finish
                        tokio::spawn(async move {
                            save_spread_latency_around_arbitrage_to_influx(
                                client2,
                                &*retention_policy,
                            )
                            .await;
                        });

                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                });
            }

            // Execute Trade, given conditions
            if starters.contains(path_names_clone[0].as_str())
                && allow_trades
                && path_names_clone.len() <= MAX_TRADES + 1
                && end_volume / min_volume > MIN_ROI
                && end_volume - min_volume > MIN_PROFIT
                && p90_latency_value < MAX_LATENCY
                && high_enough_trade_volume
            {
                log::debug!("Trade should have triggered for {:?}", path_names_clone);
                let winnings_expected = end_volume - min_volume; // Do before adjusting min_volume

                // When we want to limit trading by limiting `ordermin` per pair, will need to move above this if statement
                let balances = balances.lock().unwrap().clone();
                let balance = balances.get(&path_names_clone[0]).unwrap_or(&0.0);
                min_volume = min_volume.min(*balance * 0.8); // Only trade up to 80% of what we have, to avoid failing trades

                let fees_clone = fees.lock().unwrap().clone();
                let private_ws_arc = match &private_ws {
                    Some(ws) => Arc::clone(ws),
                    None => {
                        return Err("Can't execute trades: Private WebSocket does not exist".into())
                    }
                };
                let client_clone = Arc::clone(&client);
                let roi_expected = end_volume / min_volume - 1.;
                execute_trade(
                    path_names_clone,
                    pair_to_decimals.clone(),
                    &rates_clone,
                    min_volume,
                    &assets_to_pair,
                    pair_to_spread,
                    private_ws_arc,
                    token.expect("Token must exist to trade"),
                    &fees_clone,
                    &orders,
                    client_clone,
                    graph_id,
                    p90_latency_value,
                    winnings_expected,
                    roi_expected,
                )
                .await;
            } else {
                log::debug!("Trade did not trigger. Values are: starters contains path_names_clone[0]: {}, allow_trades: {}, path_names_clone.len() <= MAX_TRADES + 1: {}, end_volume / min_volume > MIN_ROI: {}, end_volume - min_volume > MIN_PROFIT: {}, p90_latency_value < MAX_LATENCY: {}, high_enough_trade_volume: {}",
                           starters.contains(path_names_clone[0].as_str()),
                           allow_trades,
                           path_names_clone.len(),
                           end_volume / min_volume,
                           end_volume - min_volume,
                           p90_latency_value,
                           high_enough_trade_volume);
            }
        }
    }
}

fn limiting_volume(
    path: &[String],
    rates: &HashMap<(String, String), f64>,
    volumes: &HashMap<(String, String), f64>,
) -> (f64, f64, Vec<f64>) {
    let mut min_volume = std::f64::MAX;
    let mut ending_volume = 1.;
    let mut rate_vec = Vec::new();

    for i in 0..path.len() - 1 {
        let asset1 = &path[i];
        let asset2 = &path[i + 1];

        // Find the volume for the current asset pair, in terms of the 1st asset
        let rate = match rates.get(&(asset1.clone(), asset2.clone())) {
            Some(rate) => rate,
            None => {
                let msg = format!(
                    "Expected rate for assets {} and {}, but found none. Rates are {:?} and path is {:?}",
                    asset1, asset2, rates, path
                );
                log::error!("{}", msg);
                panic!("{}", msg);
            }
        };
        let volume2 = match volumes.get(&(asset1.clone(), asset2.clone())) {
            Some(volume) => volume,
            None => {
                let msg = format!(
                    "Expected volume for assets {} and {}, but found none. Volumes are {:?} and path is {:?}",
                    asset1, asset2, volumes, path
                );
                log::error!("{}", msg);
                panic!("{}", msg);
            }
        };
        let volume1 = volume2 / rate;
        ending_volume *= rate;
        rate_vec.push(*rate);

        // Update the minimum volume if necessary
        if volume1 < min_volume {
            min_volume = volume1;
        }
        min_volume *= rate; // Regardless, convert to the next asset's terms
    }
    ending_volume *= min_volume;
    (min_volume, ending_volume, rate_vec)
}

/// If any trade in the path is below the ordermin or costmin for the pair, flag as false
/// TODO: Should consider drop in volume over the trades based on fees. For now, ignoring
fn is_trade_volume_high_enough(
    min_volume: &f64,
    path: &[String],
    pair_to_trade_mins: &PairToTradeMin,
    assets_to_pair: AssetsToPair,
    pair_to_spread: &PairToSpread,
) -> bool {
    let mut traded_vol = *min_volume;
    for i in 0..path.len() - 1 {
        let asset1 = path[i].clone();
        let asset2 = path[i + 1].clone();
        let asset1_clone = asset1.clone();
        let asset2_clone = asset2.clone();
        let pair = assets_to_pair
            .get(&(asset1, asset2))
            .expect("Path contains invalid pair");
        let spread = pair_to_spread
            .get(&pair.pair)
            .expect("Couldn't find spread");
        let trade_mins = pair_to_trade_mins
            .get(&pair.pair)
            .expect("No trade min found");

        if asset1_clone == pair.base {
            if traded_vol < trade_mins.ordermin || traded_vol * spread.bid < trade_mins.costmin {
                return false;
            }
            traded_vol *= spread.bid;
        } else if asset2_clone == pair.base {
            if traded_vol < trade_mins.costmin || traded_vol / spread.ask < trade_mins.ordermin {
                return false;
            }
            traded_vol /= spread.ask;
        } else {
            panic!("Neither asset in pair is base");
        }
    }
    true
}

fn generate_asset_to_index_map(pair_to_assets: &PairToAssets) -> HashMap<String, usize> {
    let mut asset_to_index = HashMap::new();
    let mut index = 0;
    for (
        _,
        BaseQuote {
            base: asset1,
            quote: asset2,
        },
    ) in pair_to_assets
    {
        asset_to_index.entry(asset1.clone()).or_insert_with(|| {
            index += 1;
            index - 1
        });
        asset_to_index.entry(asset2.clone()).or_insert_with(|| {
            index += 1;
            index - 1
        });
    }
    asset_to_index
}

fn prepare_graph(
    pair_to_spread: &PairToSpread,
    pair_to_assets: &PairToAssets,
    asset_to_index: &HashMap<String, usize>,
    fees: &HashMap<String, f64>,
    pair_status: &HashMap<String, bool>,
) -> (
    Vec<Edge>,
    HashMap<(String, String), f64>,
    HashMap<(String, String), f64>,
) {
    let mut exchange_rates = vec![];
    let mut rates_map = HashMap::new();
    let mut volumes_map = HashMap::new();
    for (
        pair,
        Spread {
            bid,
            ask,
            bid_volume,
            ask_volume,
            ..
        },
    ) in pair_to_spread
    {
        if let Some(BaseQuote {
            base: asset1,
            quote: asset2,
        }) = pair_to_assets.get(pair)
        {
            if let Some(status) = pair_status.get(pair) {
                if *status {
                    let index1 = *asset_to_index
                        .get(asset1)
                        .expect(&format!("Expected {} in asset_to_index", asset1));
                    let index2 = *asset_to_index
                        .get(asset2)
                        .expect(&format!("Expected {} in asset_to_index", asset2));
                    let bid_rate = bid * (1.0 - fees[pair]);
                    let ask_rate = 1.0 / (ask * (1.0 + fees[pair]));
                    exchange_rates.push(Edge {
                        src: index1,
                        dest: index2,
                        weight: -(bid_rate).ln(),
                    });
                    exchange_rates.push(Edge {
                        src: index2,
                        dest: index1,
                        weight: -(ask_rate).ln(),
                    });
                    rates_map.insert((asset1.clone(), asset2.clone()), bid_rate);
                    rates_map.insert((asset2.clone(), asset1.clone()), ask_rate);
                    volumes_map.insert((asset1.clone(), asset2.clone()), bid_volume * bid);
                    volumes_map.insert((asset2.clone(), asset1.clone()), *ask_volume);
                }
            }
        }
    }

    (exchange_rates, rates_map, volumes_map)
}

async fn save_evaluation_time_to_influx(
    client: Arc<Client>,
    retention_policy: &str,
    graph_id: i64,
    start_time: u128,
    end_time: u128,
) {
    let duration = (end_time - start_time) as f64 / 1_000_000_000.0;
    let point = Point::new("evaluation_time")
        .add_timestamp(start_time.try_into().unwrap())
        .add_tag("graph_id", Value::Integer(graph_id))
        .add_field("duration", Value::Float(duration));
    let _ = client
        .write_point(point, Some(Precision::Nanoseconds), Some(retention_policy))
        .await
        .expect("Failed to write to evaluation_time");
}

async fn save_spread_latency_around_arbitrage_to_influx(
    client: Arc<Client>,
    retention_policy: &str,
) {
    sleep(Duration::from_secs(300)).await;
    let query = format!(
        "SELECT * INTO spreads_around_arbitrage FROM {}.spread_latency WHERE time >= now() - 10m",
        retention_policy
    );
    let _ = client
        .query(&query, None)
        .await
        .expect("Saving spreads_around_arbitrage failed");
}

async fn arbitrage_details_to_influx(
    client: Arc<Client>,
    graph_id: i64,
    limited_volume: f64,
    ending_volume: f64,
    volume_units: String,
    path: Vec<String>,
    rates: Vec<f64>,
) {
    let point = Point::new("arbitrage_details")
        .add_tag("graph_id", Value::Integer(graph_id))
        .add_field("limiting_volume", Value::Float(limited_volume))
        .add_field("ending_volume", Value::Float(ending_volume))
        .add_field("volume_units", Value::String(volume_units))
        .add_field("path", Value::String(path.join(", ")))
        .add_field(
            "rates",
            Value::String(
                rates
                    .iter()
                    .map(|r| r.to_string())
                    .collect::<Vec<String>>()
                    .join(", "),
            ),
        );
    let _ = client
        .write_point(point, Some(Precision::Nanoseconds), None)
        .await
        .expect("Failed to write to arbitrage_details");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_generate_asset_to_index_map() {
        let mut pair_to_assets = HashMap::new();
        pair_to_assets.insert(
            "pair1".to_string(),
            BaseQuote {
                base: "asset1".to_string(),
                quote: "asset2".to_string(),
            },
        );
        pair_to_assets.insert(
            "pair2".to_string(),
            BaseQuote {
                base: "asset2".to_string(),
                quote: "asset3".to_string(),
            },
        );

        let asset_to_index = generate_asset_to_index_map(&pair_to_assets);

        assert!(
            asset_to_index.get("asset1").unwrap() >= &0
                && asset_to_index.get("asset1").unwrap() <= &2
        );
        assert!(
            asset_to_index.get("asset2").unwrap() >= &0
                && asset_to_index.get("asset2").unwrap() <= &2
        );
        assert!(
            asset_to_index.get("asset3").unwrap() >= &0
                && asset_to_index.get("asset3").unwrap() <= &2
        );
    }

    #[test]
    fn test_prepare_graph() {
        let mut pair_to_spread = HashMap::new();
        pair_to_spread.insert(
            "pair1".to_string(),
            Spread {
                bid: 1.0,
                ask: 2.0,
                kraken_ts: 123.0,
                bid_volume: 0.0,
                ask_volume: 0.0,
            },
        );
        pair_to_spread.insert(
            "pair2".to_string(),
            Spread {
                bid: 3.0,
                ask: 4.0,
                kraken_ts: 123.0,
                bid_volume: 0.0,
                ask_volume: 0.0,
            },
        );

        let mut pair_to_assets = HashMap::new();
        pair_to_assets.insert(
            "pair1".to_string(),
            BaseQuote {
                base: "asset1".to_string(),
                quote: "asset2".to_string(),
            },
        );
        pair_to_assets.insert(
            "pair2".to_string(),
            BaseQuote {
                base: "asset2".to_string(),
                quote: "asset3".to_string(),
            },
        );

        let mut fees = HashMap::new();
        fees.insert("pair1".to_string(), 0.0026);
        fees.insert("pair2".to_string(), 0.0026);

        let mut pair_status = HashMap::new();
        pair_status.insert("pair1".to_string(), true);
        pair_status.insert("pair2".to_string(), true);

        let asset_to_index = generate_asset_to_index_map(&pair_to_assets);

        let (edges, _rate_map, _volume_map) = prepare_graph(
            &pair_to_spread,
            &pair_to_assets,
            &asset_to_index,
            &fees,
            &pair_status,
        );

        assert_eq!(edges.len(), 4);

        let edge_from_asset1_to_asset2 = edges.iter().find(|edge| {
            edge.src == *asset_to_index.get("asset1").unwrap()
                && edge.dest == *asset_to_index.get("asset2").unwrap()
        });
        assert!(
            edge_from_asset1_to_asset2.is_some(),
            "Expected an edge from asset1 to asset2"
        );
        let edge = edge_from_asset1_to_asset2.unwrap();
        assert_eq!(edge.weight, -((1.0 * (1.0 - 0.0026)) as f64).ln(), "Expected the weight of the edge from asset1 to asset2 to be the natural logarithm of the negative bid rate");
    }

    #[test]
    fn test_minimum_volume_for_each_asset() {
        let path = vec![
            "ABC".to_string(),
            "DEF".to_string(),
            "GEH".to_string(),
            "ABC".to_string(),
        ];
        let mut rates = HashMap::new();
        rates.insert(("ABC".to_string(), "DEF".to_string()), 0.5);
        rates.insert(("DEF".to_string(), "GEH".to_string()), 2.0);
        rates.insert(("GEH".to_string(), "ABC".to_string()), 2.0);
        let mut volumes = HashMap::new();
        volumes.insert(("ABC".to_string(), "DEF".to_string()), 3.0);
        volumes.insert(("DEF".to_string(), "GEH".to_string()), 1.0);
        volumes.insert(("GEH".to_string(), "ABC".to_string()), 4.0);

        let (min_volume, ending_volume, _) = limiting_volume(&path, &rates, &volumes);
        assert_eq!(min_volume, 2.0); // Expected minimum volume is 1.0
        assert_eq!(ending_volume, 4.0); // Putting in $2 yields $4
    }
}
