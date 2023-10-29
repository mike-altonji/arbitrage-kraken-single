use std::env;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// use std::time::Instant;
use tokio::time::Duration;
use crate::graph_algorithms::{bellman_ford_negative_cycle, Edge};

const FEE: f64 = 0.0026;

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

    loop {
        // let start_time = Instant::now();
        let asset_pairs = shared_asset_pairs.lock().unwrap().clone();
        let (n, rate_edges, rate_map, volume_map) = prepare_graph(&asset_pairs, &pair_to_assets);
        // let duration = start_time.elapsed();
        // println!("{:?}", duration);
        let path = bellman_ford_negative_cycle(n, &rate_edges, 0); // This assumes source as 0, you can change if needed
        if let Some(negative_cycle) = path {
            let _volume = limiting_volume(&negative_cycle, &rate_map, &volume_map);
            let message = format!("Arbitrage opportunity at cycle: {:?}", negative_cycle);
            send_telegram_message(&bot_token, &chat_id, &message).await?;
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

async fn send_telegram_message(bot_token: &str, chat_id: &str, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}", bot_token, chat_id, message);
    let _response = reqwest::Client::new().post(&url).send().await?;
    Ok(())
}

fn prepare_graph(
    asset_pairs: &HashMap<String, (f64, f64, f64, f64)>, 
    pair_to_assets: &HashMap<String, (String, String)>
) -> (usize, Vec<Edge>, HashMap<(usize, usize), f64>, HashMap<(usize, usize), f64>) {
    let mut asset_to_index = HashMap::new();
    let mut index = 0;
    let mut exchange_rates = vec![];
    let mut rates_map = HashMap::new();
    let mut volumes_map = HashMap::new();
    for (pair, (bid, ask, bid_volume, ask_volume)) in asset_pairs {
        if let Some((asset1, asset2)) = pair_to_assets.get(pair) {
            let index1 = *asset_to_index.entry(asset1.clone()).or_insert_with(|| { index += 1; index - 1 });
            let index2 = *asset_to_index.entry(asset2.clone()).or_insert_with(|| { index += 1; index - 1 });
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
    (index, exchange_rates, rates_map, volumes_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_prepare_graph() {
        let mut asset_pairs = HashMap::new();
        asset_pairs.insert("pair1".to_string(), (1.0, 2.0, 0.0, 0.0));
        asset_pairs.insert("pair2".to_string(), (3.0, 4.0, 0.0, 0.0));

        let mut pair_to_assets = HashMap::new();
        pair_to_assets.insert("pair1".to_string(), ("asset1".to_string(), "asset2".to_string()));
        pair_to_assets.insert("pair2".to_string(), ("asset2".to_string(), "asset3".to_string()));

        let (n, edges, _rate_map, _volume_map) = prepare_graph(&asset_pairs, &pair_to_assets);

        assert_eq!(n, 3);
        assert_eq!(edges.len(), 4);
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
}