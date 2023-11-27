use crate::graph_algorithms::{bellman_ford_negative_cycle, Edge};
use crate::kraken_private::execute_trade;
use influx_db_client::{reqwest::Url, Client, Point, Precision, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

const FEE: f64 = 0.0026;
const TRADEABLE_ASSETS: [&str; 2] = ["USD", "EUR"]; // Cycle must contain one of these to execute a trade.

pub async fn evaluate_arbitrage_opportunities(
    pair_to_assets: HashMap<String, (String, String)>,
    shared_asset_pairs: Arc<Mutex<HashMap<String, (f64, f64, f64, f64, f64)>>>,
    pair_status: Arc<Mutex<HashMap<String, bool>>>,
    public_online: Arc<Mutex<bool>>,
    graph_id: i64,
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

    // Give shared_asset_pairs time to populate
    tokio::time::sleep(Duration::from_secs(3)).await;

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

        let asset_pairs = shared_asset_pairs.lock().unwrap().clone();
        let pair_status_clone = pair_status.lock().unwrap().clone();
        let (rate_edges, rate_map, volume_map) = prepare_graph(
            &asset_pairs,
            &pair_to_assets,
            &asset_to_index,
            &pair_status_clone,
        );
        let path = bellman_ford_negative_cycle(n, &rate_edges, 0);

        // Log a subsample of events every N iterations
        if counter == 1000 {
            let end_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let client0 = Arc::clone(&client);
            let retention_policy = Arc::clone(&retention_policy_clone);
            tokio::spawn(async move {
                save_evaluation_time_to_influx(
                    client0,
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
        if let Some(mut negative_cycle) = path {
            rotate_path(
                &mut negative_cycle,
                &asset_to_index,
                &Vec::from(TRADEABLE_ASSETS),
            );
            let (min_volume, end_volume, rates) =
                limiting_volume(&negative_cycle, &rate_map, &volume_map);
            let asset_names: Vec<String> = negative_cycle
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

            // Log and send message at most every 5 seconds
            let asset_names_clone = asset_names.clone();
            let retention_policy = Arc::clone(&retention_policy_clone);
            let client1 = Arc::clone(&client);
            let client2 = Arc::clone(&client);
            let sem_clone = Arc::clone(&semaphore);
            tokio::spawn(async move {
                if let Ok(_permit) = sem_clone.try_acquire() {
                    arbitrage_details_to_influx(
                        client2,
                        graph_id,
                        min_volume,
                        end_volume,
                        asset_names[0].to_string(),
                        asset_names,
                        rates,
                    )
                    .await;

                    // Log +/- 5 minutes of raw data. Don't need to wait for it to finish
                    tokio::spawn(async move {
                        save_spread_latency_around_arbitrage_to_influx(client1, &*retention_policy)
                            .await;
                    });

                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });

            // Execute Trade TODO: This is a dummy for now, need real logic
            if TRADEABLE_ASSETS
                .iter()
                .any(|&asset| asset_names_clone.contains(&asset.to_string()))
            {
                // execute_trade(&asset_names_clone[0], &asset_names_clone[1], min_volume).await?;
            }
        }
    }
}

fn limiting_volume(
    path: &[usize],
    rates: &HashMap<(usize, usize), f64>,
    volumes: &HashMap<(usize, usize), f64>,
) -> (f64, f64, Vec<f64>) {
    let mut min_volume = std::f64::MAX;
    let mut ending_volume = 1.;
    let mut rate_vec = Vec::new();

    for i in 0..path.len() - 1 {
        let asset1 = path[i];
        let asset2 = path[i + 1];

        // Find the volume for the current asset pair, in terms of the 1st asset
        let rate = match rates.get(&(asset1, asset2)) {
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
        let volume2 = match volumes.get(&(asset1, asset2)) {
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

fn rotate_path(
    negative_cycle: &mut Vec<usize>,
    asset_to_index: &HashMap<String, usize>,
    prioritized_assets: &Vec<&str>,
) {
    negative_cycle.pop(); // Remove the last element in the path
                          // Rotate the path until one of the `prioritized_assets` is first
    for asset in prioritized_assets {
        if let Some(asset_index) = negative_cycle.iter().position(|&i| {
            asset_to_index
                .iter()
                .find(|&(_, &v)| v == i)
                .unwrap()
                .0
                .as_str()
                == *asset
        }) {
            negative_cycle.rotate_left(asset_index);
            break;
        }
    }
    negative_cycle.push(*negative_cycle.first().unwrap()); // Add the first asset to the end of the path
}

fn generate_asset_to_index_map(
    pair_to_assets: &HashMap<String, (String, String)>,
) -> HashMap<String, usize> {
    let mut asset_to_index = HashMap::new();
    let mut index = 0;
    for (_, (asset1, asset2)) in pair_to_assets {
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
    asset_pairs: &HashMap<String, (f64, f64, f64, f64, f64)>,
    pair_to_assets: &HashMap<String, (String, String)>,
    asset_to_index: &HashMap<String, usize>,
    pair_status: &HashMap<String, bool>,
) -> (
    Vec<Edge>,
    HashMap<(usize, usize), f64>,
    HashMap<(usize, usize), f64>,
) {
    let mut exchange_rates = vec![];
    let mut rates_map = HashMap::new();
    let mut volumes_map = HashMap::new();
    for (pair, (bid, ask, _, bid_volume, ask_volume)) in asset_pairs {
        if let Some((asset1, asset2)) = pair_to_assets.get(pair) {
            if let Some(status) = pair_status.get(pair) {
                if *status {
                    let index1 = *asset_to_index
                        .get(asset1)
                        .expect(&format!("Expected {} in asset_to_index", asset1));
                    let index2 = *asset_to_index
                        .get(asset2)
                        .expect(&format!("Expected {} in asset_to_index", asset2));
                    let bid_rate = bid * (1.0 - FEE);
                    let ask_rate = 1.0 / (ask * (1.0 + FEE));
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
                    rates_map.insert((index1, index2), bid_rate);
                    rates_map.insert((index2, index1), ask_rate);
                    volumes_map.insert((index1, index2), bid_volume * bid);
                    volumes_map.insert((index2, index1), *ask_volume);
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
            ("asset1".to_string(), "asset2".to_string()),
        );
        pair_to_assets.insert(
            "pair2".to_string(),
            ("asset2".to_string(), "asset3".to_string()),
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
        let mut asset_pairs = HashMap::new();
        asset_pairs.insert("pair1".to_string(), (1.0, 2.0, 123., 0.0, 0.0));
        asset_pairs.insert("pair2".to_string(), (3.0, 4.0, 123., 0.0, 0.0));

        let mut pair_to_assets = HashMap::new();
        pair_to_assets.insert(
            "pair1".to_string(),
            ("asset1".to_string(), "asset2".to_string()),
        );
        pair_to_assets.insert(
            "pair2".to_string(),
            ("asset2".to_string(), "asset3".to_string()),
        );

        let mut pair_status = HashMap::new();
        pair_status.insert("pair1".to_string(), true);
        pair_status.insert("pair2".to_string(), true);

        let asset_to_index = generate_asset_to_index_map(&pair_to_assets);

        let (edges, _rate_map, _volume_map) =
            prepare_graph(&asset_pairs, &pair_to_assets, &asset_to_index, &pair_status);

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
        assert_eq!(edge.weight, -(1.0 * (1.0 - FEE)).ln(), "Expected the weight of the edge from asset1 to asset2 to be the natural logarithm of the negative bid rate");
    }

    #[test]
    fn test_minimum_volume_for_each_asset() {
        let path = vec![0, 1, 2, 0];
        let mut rates = HashMap::new();
        rates.insert((0, 1), 0.5);
        rates.insert((1, 2), 2.0);
        rates.insert((2, 0), 2.0);
        let mut volumes = HashMap::new();
        volumes.insert((0, 1), 3.0);
        volumes.insert((1, 2), 1.0);
        volumes.insert((2, 0), 4.0);

        let (min_volume, ending_volume, _) = limiting_volume(&path, &rates, &volumes);
        assert_eq!(min_volume, 2.0); // Expected minimum volume is 1.0
        assert_eq!(ending_volume, 4.0); // Putting in $2 yields $4
    }

    #[test]
    fn test_rotate_path() {
        let mut negative_cycle = vec![0, 1, 2, 3, 4, 0];
        let mut asset_to_index = HashMap::new();
        asset_to_index.insert("asset0".to_string(), 0);
        asset_to_index.insert("asset1".to_string(), 1);
        asset_to_index.insert("asset2".to_string(), 2);
        asset_to_index.insert("asset3".to_string(), 3);
        asset_to_index.insert("asset4".to_string(), 4);

        rotate_path(
            &mut negative_cycle,
            &asset_to_index,
            &vec!["asset999", "asset2"],
        );

        assert_eq!(negative_cycle, vec![2, 3, 4, 0, 1, 2]);
    }
}
