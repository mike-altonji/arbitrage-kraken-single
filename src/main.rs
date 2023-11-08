use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use dotenv::dotenv;
use futures::future::select_all;

mod kraken;
mod evaluate_arbitrage;
mod graph_algorithms;

#[tokio::main]
async fn main() {
    dotenv().ok();
    // Set up logging to write to `application.log`.
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time invalid");
    let timestamp = since_the_epoch.as_secs();
    let log_config = log4rs::append::file::FileAppender::builder()
        .build(format!("logs/arbitrage_log_{}.log", timestamp))
        .unwrap();
    
    let log_config = log4rs::config::Config::builder()
        .appender(log4rs::config::Appender::builder().build("default", Box::new(log_config)))
        .build(log4rs::config::Root::builder().appender("default").build(log::LevelFilter::Info))
        .unwrap();

    log4rs::init_config(log_config).unwrap();

    // Pull asset pairs and initialize bids/asks
    let csv_files = vec!["resources/asset_pairs_a1.csv", "resources/asset_pairs_a2.csv", "resources/asset_pairs_b1.csv", "resources/asset_pairs_b2.csv"];
    let mut pairs_to_assets_vec = Vec::new();
    let mut shared_asset_pairs_vec = Vec::new();
    
    for csv_file in csv_files {
        let pair_to_assets = kraken::asset_pairs_to_pull(csv_file).await.expect("Failed to get asset pairs");
        let shared_asset_pairs = Arc::new(Mutex::new(HashMap::new()));
        pairs_to_assets_vec.push(pair_to_assets);
        shared_asset_pairs_vec.push(shared_asset_pairs);
    }

    // Unique set of all pairs, so we just have 1 subscription to the Kraken websocket
    let mut all_pairs = HashSet::new();
    for pair_to_assets in &pairs_to_assets_vec {
        for pair in pair_to_assets.keys() {
            all_pairs.insert(pair.clone());
        }
    }

    // Keep bids/asks up to date
    let fetch_handle = {
        let all_pairs_clone = all_pairs.clone();
        let shared_asset_pairs_vec_clone = shared_asset_pairs_vec.clone();
        tokio::spawn(async move {
            kraken::fetch_kraken_data_ws(all_pairs_clone, shared_asset_pairs_vec_clone).await.expect("Failed to fetch data");
        })
    };

    // Search for arbitrage opportunities, for each graph
    let mut evaluate_handles = Vec::new();
    for i in 0..pairs_to_assets_vec.len() {
        let evaluate_handle = {
            let pair_to_assets_clone = pairs_to_assets_vec[i].clone();
            let shared_asset_pairs_clone = shared_asset_pairs_vec[i].clone();
            tokio::spawn(async move {
                let _ = evaluate_arbitrage::evaluate_arbitrage_opportunities(pair_to_assets_clone, shared_asset_pairs_clone).await;
            })
        };
        evaluate_handles.push(evaluate_handle);
    }

    // Wait for both tasks to complete (this will likely never happen given the current logic)
    let mut all_handles = vec![Box::pin(fetch_handle)];
    for handle in evaluate_handles {
        all_handles.push(Box::pin(handle));
    }
    
    let (result, _index, _remaining) = select_all(all_handles).await;
    match result {
        Ok(_) => println!("A task completed successfully"),
        Err(e) => eprintln!("A task failed with error: {:?}", e),
    }
}
