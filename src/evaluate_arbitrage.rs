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
        let (n, rates, _volumes) = prepare_graph(&asset_pairs, &pair_to_assets);
        // let duration = start_time.elapsed();
        // println!("{:?}", duration);
        let path = bellman_ford_negative_cycle(n, &rates, 0); // This assumes source as 0, you can change if needed
        if let Some(negative_cycle) = path {
            let message = format!("Arbitrage opportunity at cycle: {:?}", negative_cycle);
            send_telegram_message(&bot_token, &chat_id, &message).await?;
        }
    }
}

async fn send_telegram_message(bot_token: &str, chat_id: &str, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}", bot_token, chat_id, message);
    let _response = reqwest::Client::new().post(&url).send().await?;
    Ok(())
}

fn prepare_graph(
    asset_pairs: &HashMap<String, (f64, f64, f64, f64)>, 
    pair_to_assets: &HashMap<String, (String, String)>
) -> (usize, Vec<Edge>, Vec<Edge>) {
    let mut asset_to_index = HashMap::new();
    let mut index = 0;
    let mut exchange_rates = vec![];
    let mut available_volumes = vec![];
    for (pair, (bid, ask, bid_volume, ask_volume)) in asset_pairs {
        if let Some((asset1, asset2)) = pair_to_assets.get(pair) {
            let index1 = *asset_to_index.entry(asset1.clone()).or_insert_with(|| { index += 1; index - 1 });
            let index2 = *asset_to_index.entry(asset2.clone()).or_insert_with(|| { index += 1; index - 1 });
            let bid_rate = -((bid * (1.0 - FEE)).ln());
            let ask_rate = -((1.0 / (ask * (1.0 + FEE))).ln());
            exchange_rates.push(Edge { src: index1, dest: index2, weight: bid_rate });
            exchange_rates.push(Edge { src: index2, dest: index1, weight: ask_rate });
            available_volumes.push(Edge {src: index1, dest: index2, weight: bid_volume * bid});
            available_volumes.push(Edge {src: index2, dest: index1, weight: *ask_volume});
        }
    }
    (index, exchange_rates, available_volumes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_graph() {
        let mut asset_pairs = HashMap::new();
        asset_pairs.insert("pair1".to_string(), (1.0, 2.0, 0.0, 0.0));
        asset_pairs.insert("pair2".to_string(), (3.0, 4.0, 0.0, 0.0));

        let mut pair_to_assets = HashMap::new();
        pair_to_assets.insert("pair1".to_string(), ("asset1".to_string(), "asset2".to_string()));
        pair_to_assets.insert("pair2".to_string(), ("asset2".to_string(), "asset3".to_string()));

        let (n, edges, _volumes) = prepare_graph(&asset_pairs, &pair_to_assets);

        assert_eq!(n, 3);
        assert_eq!(edges.len(), 4);
    }
}