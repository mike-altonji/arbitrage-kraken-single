use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use dotenv::dotenv;
use futures::future::select_all;
use tokio::time::sleep;

mod kraken;
mod evaluate_arbitrage;
mod graph_algorithms;
mod telegram;

use crate::telegram::send_telegram_message;

#[tokio::main]
async fn main() {
    // Initialize setup
    dotenv().ok();
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
    send_telegram_message("🚀 Launching arbitrage trader.").await.expect("Launch message failed to send");

    // Loop allows retries
    let mut retry = 0;
    while retry <= 5 {
        retry += 1;
        if retry > 1 {
            sleep(Duration::from_secs(10)).await;
            send_telegram_message(&format!("Restart arbitrage trader #{}", retry - 1)).await.expect("Launch message failed to send");
        }
        

        // Pull asset pairs and initialize bids/asks
        let paths = std::fs::read_dir("resources").expect("Failed to read directory");
        let csv_files: Vec<_> = paths
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().and_then(std::ffi::OsStr::to_str) == Some("csv"))
            .map(|e| e.path().to_str().unwrap().to_string())
            .collect();
        let mut pairs_to_assets_vec = Vec::new();
        let mut shared_asset_pairs_vec = Vec::new();
        
        for csv_file in csv_files {
            let pair_to_assets = kraken::asset_pairs_to_pull(&csv_file).await.expect("Failed to get asset pairs");
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
                    let _ = evaluate_arbitrage::evaluate_arbitrage_opportunities(pair_to_assets_clone, shared_asset_pairs_clone, i as i64).await;
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
            Ok(_) => send_telegram_message("Code died for some reason. Waiting 10 seconds, then restarting.").await.unwrap(),
            Err(e) => {
                let error_message = format!("A task failed with error: {:?}", e);
                log::info!("{}", error_message);
                send_telegram_message(&error_message).await.expect("Failure message failed to send");
            },
        }
    }
    send_telegram_message("Too many retries: Exiting the program.").await.expect("Failed to send");
    std::process::exit(1);
}
