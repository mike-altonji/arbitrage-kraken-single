use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use dotenv::dotenv;

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

    let pair_to_assets = kraken::asset_pairs_to_pull().await.expect("Failed to get asset pairs");
    let shared_asset_pairs = Arc::new(Mutex::new(HashMap::new()));

    let fetch_handle = {
        let pair_to_assets_clone = pair_to_assets.clone();
        let shared_asset_pairs_clone = shared_asset_pairs.clone();
        tokio::spawn(async move {
            kraken::fetch_kraken_data_ws(pair_to_assets_clone, shared_asset_pairs_clone).await.expect("Failed to fetch data");
        })
    };

    let evaluate_handle = {
        let pair_to_assets_clone = pair_to_assets.clone();
        let shared_asset_pairs_clone = shared_asset_pairs.clone();
        tokio::spawn(async move {
            let _ = evaluate_arbitrage::evaluate_arbitrage_opportunities(pair_to_assets_clone, shared_asset_pairs_clone).await;
        })
    };

    // Wait for both tasks to complete (this will likely never happen given the current logic)
    let _ = tokio::try_join!(fetch_handle, evaluate_handle);
}
