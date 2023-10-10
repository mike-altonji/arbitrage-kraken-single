use std::env;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// use std::time::Instant;
use tokio::time::Duration;
use crate::graph_algorithms::{bellman_ford_negative_cycle, Edge};

const FEE: f64 = 0.0026;

pub async fn evaluate_arbitrage_opportunities(
    pair_to_assets: HashMap<String, (String, String)>,
    shared_asset_pairs: Arc<Mutex<HashMap<String, (f64, f64)>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bot_token = env::var("TELEGRAM_BOT_TOKEN").expect("TELEGRAM_BOT_TOKEN must be set");
    let chat_id = env::var("TELEGRAM_CHAT_ID").expect("TELEGRAM_CHAT_ID must be set");

    // Give shared_asset_pairs time to populate
    tokio::time::sleep(Duration::from_secs(3)).await;
    let message = format!("🚀 Launching websocket-based, Rust arbitrage trader.");
    let url = format!("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}", bot_token, chat_id, message);
    let _response = reqwest::Client::new().post(&url).send().await?;

    loop {
        // let start_time = Instant::now();
        let asset_pairs = shared_asset_pairs.lock().unwrap().clone();
        let (n, edges) = prepare_graph(&asset_pairs, &pair_to_assets);
        let node = bellman_ford_negative_cycle(n, &edges, 0); // This assumes source as 0, you can change if needed
        // let duration = start_time.elapsed();
        // println!("{:?}", duration);
        if let Some(node_index) = node {
            let message = format!("Arbitrage opportunity at node {}", node_index);
            let url = format!("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}", bot_token, chat_id, message);
            let _response = reqwest::Client::new().post(&url).send().await?;
        }
    }
}


fn prepare_graph(asset_pairs: &HashMap<String, (f64, f64)>, pair_to_assets: &HashMap<String, (String, String)>) -> (usize, Vec<Edge>) {
    let mut asset_to_index = HashMap::new();
    let mut index = 0;
    let mut edges = vec![];
    for (pair, (bid, ask)) in asset_pairs {
        if let Some((asset1, asset2)) = pair_to_assets.get(pair) {
            let index1 = *asset_to_index.entry(asset1.clone()).or_insert_with(|| { index += 1; index - 1 });
            let index2 = *asset_to_index.entry(asset2.clone()).or_insert_with(|| { index += 1; index - 1 });
            edges.push(Edge { src: index1, dest: index2, weight: bid * (1.0 - FEE) });
            edges.push(Edge { src: index2, dest: index1, weight: 1.0 / (ask * (1.0 + FEE)) });
        }
    }
    (index, edges)
}
