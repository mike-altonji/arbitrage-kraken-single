use std::env;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// use std::time::Instant;
use tokio::time::Duration;
use crate::graph_algorithms::{bellman_ford_negative_cycle, Edge};
use crate::kraken::execute_trade;

const FEE: f64 = 0.0026;
const TRADEABLE_ASSET: &str = "ZUSD";

pub async fn evaluate_arbitrage_opportunities(
    pair_to_assets: HashMap<String, (String, String)>,
    shared_asset_pairs: Arc<Mutex<HashMap<String, (f64, f64, f64, f64)>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bot_token = env::var("TELEGRAM_BOT_TOKEN").expect("TELEGRAM_BOT_TOKEN must be set");
    let chat_id = env::var("TELEGRAM_CHAT_ID").expect("TELEGRAM_CHAT_ID must be set");

    // Give shared_asset_pairs time to populate
    tokio::time::sleep(Duration::from_secs(3)).await;
    let message = "🚀 Launching websocket-based, Rust arbitrage trader.";
    send_telegram_message(&bot_token, &chat_id, &message).await?;

    let asset_to_index = generate_asset_to_index_map(&pair_to_assets);
    let n = asset_to_index.len();
    log::info!("Asset to index map: {:?}", asset_to_index);

    loop {
        // let start_time = Instant::now();
        let asset_pairs = shared_asset_pairs.lock().unwrap().clone();
        let (rate_edges, rate_map, volume_map) = prepare_graph(&asset_pairs, &pair_to_assets, &asset_to_index);
        // let duration = start_time.elapsed();
        // println!("{:?}", duration);
        let path = bellman_ford_negative_cycle(n, &rate_edges, 0); // This assumes source as 0, you can change if needed
        if let Some(mut negative_cycle) = path {
            rotate_path(&mut negative_cycle, &asset_to_index, TRADEABLE_ASSET);  // Set `TRADEABLE_ASSET` to base unit
            let volume = limiting_volume(&negative_cycle, &rate_map, &volume_map);
            let asset_names: Vec<String> = negative_cycle.iter().map(|&i| asset_to_index.iter().find(|&(_, &v)| v == i).unwrap().0.clone()).collect();
            let message = format!("Arbitrage opportunity at cycle: {:?}\n\nLimiting volume: ${} {}", asset_names, volume, asset_names[0]);
            send_telegram_message(&bot_token, &chat_id, &message).await?;
            if asset_names.contains(&TRADEABLE_ASSET.to_string()) {
                execute_trade(&asset_names[0], &asset_names[1], volume).await?;
            }
        }
    }
}

fn limiting_volume(path: &[usize], rates: &HashMap<(usize, usize), f64>, volumes: &HashMap<(usize, usize), f64>) -> f64 {
    let mut min_volume = std::f64::MAX;

    for i in 0..path.len() - 1 {
        let asset1 = path[i];
        let asset2 = path[i + 1];

        // Find the volume for the current asset pair, in terms of the 1st asset
        let rate = rates.get(&(asset1, asset2)).expect("Expected rate");
        let volume2 =  volumes.get(&(asset1, asset2)).expect("Expected volume");
        let volume1 = volume2 / rate;

        // Update the minimum volume if necessary
        if volume1 < min_volume {
            min_volume = volume1;
        }
        min_volume *= rate;  // Regardless, convert to the next asset's terms
    }

    min_volume  // Return the volume in terms of the 1st asset in the path. TODO: must be USD, or whatever currency we own.
}

fn rotate_path(negative_cycle: &mut Vec<usize>, asset_to_index: &HashMap<String, usize>, asset: &str) {
    negative_cycle.pop();  // Remove the last element in the path
     // Rotate the path until `asset` is first
    if let Some(usd_index) = negative_cycle.iter().position(|&i| asset_to_index.iter().find(|&(_, &v)| v == i).unwrap().0 == asset) {
        negative_cycle.rotate_left(usd_index);
    }
    negative_cycle.push(*negative_cycle.first().unwrap());  // Add `asset` to the end of the path
}

async fn send_telegram_message(bot_token: &str, chat_id: &str, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}", bot_token, chat_id, message);
    let _response = reqwest::Client::new().post(&url).send().await?;
    Ok(())
}

fn generate_asset_to_index_map(pair_to_assets: &HashMap<String, (String, String)>) -> HashMap<String, usize> {
    let mut asset_to_index = HashMap::new();
    let mut index = 0;
    for (_, (asset1, asset2)) in pair_to_assets {
        asset_to_index.entry(asset1.clone()).or_insert_with(|| { index += 1; index - 1 });
        asset_to_index.entry(asset2.clone()).or_insert_with(|| { index += 1; index - 1 });
    }
    asset_to_index
}

fn prepare_graph(
    asset_pairs: &HashMap<String, (f64, f64, f64, f64)>, 
    pair_to_assets: &HashMap<String, (String, String)>,
    asset_to_index: &HashMap<String, usize>
) -> (Vec<Edge>, HashMap<(usize, usize), f64>, HashMap<(usize, usize), f64>) {
    let mut exchange_rates = vec![];
    let mut rates_map = HashMap::new();
    let mut volumes_map = HashMap::new();
    for (pair, (bid, ask, bid_volume, ask_volume)) in asset_pairs {
        if let Some((asset1, asset2)) = pair_to_assets.get(pair) {
            let index1 = *asset_to_index.get(asset1).expect(&format!("Expected {} in asset_to_index", asset1));
            let index2 = *asset_to_index.get(asset2).expect(&format!("Expected {} in asset_to_index", asset2));
            let bid_rate = bid * (1.0 - FEE);
            let ask_rate = 1.0 / (ask * (1.0 + FEE));
            exchange_rates.push(Edge { src: index1, dest: index2, weight: -(bid_rate).ln() });
            exchange_rates.push(Edge { src: index2, dest: index1, weight: -(ask_rate).ln() });
            rates_map.insert((index1, index2), bid_rate);
            rates_map.insert((index2, index1), ask_rate);
            volumes_map.insert((index1, index2), bid_volume * bid);
            volumes_map.insert((index2, index1), *ask_volume);
        }
    }

    (exchange_rates, rates_map, volumes_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_generate_asset_to_index_map() {
        let mut pair_to_assets = HashMap::new();
        pair_to_assets.insert("pair1".to_string(), ("asset1".to_string(), "asset2".to_string()));
        pair_to_assets.insert("pair2".to_string(), ("asset2".to_string(), "asset3".to_string()));

        let asset_to_index = generate_asset_to_index_map(&pair_to_assets);

        assert!(asset_to_index.get("asset1").unwrap() >= &0 && asset_to_index.get("asset1").unwrap() <= &2);
        assert!(asset_to_index.get("asset2").unwrap() >= &0 && asset_to_index.get("asset2").unwrap() <= &2);
        assert!(asset_to_index.get("asset3").unwrap() >= &0 && asset_to_index.get("asset3").unwrap() <= &2);
}

    #[test]
    fn test_prepare_graph() {
        let mut asset_pairs = HashMap::new();
        asset_pairs.insert("pair1".to_string(), (1.0, 2.0, 0.0, 0.0));
        asset_pairs.insert("pair2".to_string(), (3.0, 4.0, 0.0, 0.0));

        let mut pair_to_assets = HashMap::new();
        pair_to_assets.insert("pair1".to_string(), ("asset1".to_string(), "asset2".to_string()));
        pair_to_assets.insert("pair2".to_string(), ("asset2".to_string(), "asset3".to_string()));

        let asset_to_index = generate_asset_to_index_map(&pair_to_assets);

        let (edges, _rate_map, _volume_map) = prepare_graph(&asset_pairs, &pair_to_assets, &asset_to_index);

        assert_eq!(edges.len(), 4);
        
        let edge_from_asset1_to_asset2 = edges.iter().find(|edge| edge.src == *asset_to_index.get("asset1").unwrap() && edge.dest == *asset_to_index.get("asset2").unwrap());
        assert!(edge_from_asset1_to_asset2.is_some(), "Expected an edge from asset1 to asset2");
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

        let min_volume = limiting_volume(&path, &rates, &volumes);
        assert_eq!(min_volume, 2.0);  // Expected minimum volume is 1.0
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

        rotate_path(&mut negative_cycle, &asset_to_index, "asset2");

        assert_eq!(negative_cycle, vec![2, 3, 4, 0, 1, 2]);
    }
}