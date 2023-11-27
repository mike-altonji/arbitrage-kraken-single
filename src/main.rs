use dotenv::dotenv;
use futures::future::select_all;
use log4rs::{append::file::FileAppender, config};
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

mod evaluate_arbitrage;
mod graph_algorithms;
mod kraken;
mod kraken_private;
mod telegram;

use crate::telegram::send_telegram_message;

#[tokio::main]
async fn main() {
    // Initialize setup
    dotenv().ok();
    let args: Vec<String> = env::args().collect();
    let allow_trades = args.contains(&"--trade".to_string());
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time invalid");
    let timestamp = since_the_epoch.as_secs();
    let log_config = FileAppender::builder()
        .build(format!("logs/arbitrage_log_{}.log", timestamp))
        .expect("Unable to build log file");
    let log_config = config::Config::builder()
        .appender(config::Appender::builder().build("default", Box::new(log_config)))
        .build(
            config::Root::builder()
                .appender("default")
                .build(log::LevelFilter::Info),
        )
        .expect("Unable to build log file");
    log4rs::init_config(log_config).expect("Unable to build log file");
    if allow_trades {
        send_telegram_message("🚀 Launching Kraken arbitrage: Trade mode").await;
    } else {
        send_telegram_message("🚀 Launching Kraken arbitrage: Evaluation-only mode").await;
    }

    // Loop allows retries
    let mut retry = 0;
    while retry <= 5 {
        retry += 1;
        sleep(Duration::from_secs(10)).await;

        // Pull asset pairs and initialize bids/asks
        let paths = std::fs::read_dir("resources").expect("Failed to read directory");
        let csv_files: Vec<_> = paths
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().and_then(std::ffi::OsStr::to_str) == Some("csv"))
            .map(|e| e.path().to_str().unwrap().to_string())
            .collect();
        let mut pairs_to_assets_vec = Vec::new();
        let mut shared_asset_pairs_vec = Vec::new();
        let pair_status: Arc<Mutex<HashMap<String, bool>>> = Arc::new(Mutex::new(HashMap::new()));
        let public_online = Arc::new(Mutex::new(false));
        // let private_stream = Arc::new(Mutex::new(None));
        let auth_token = Arc::new(Mutex::new(None));

        for csv_file in csv_files {
            let pair_to_assets = kraken::asset_pairs_to_pull(&csv_file)
                .await
                .expect("Failed to get asset pairs");
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
            let pairs_to_assets_vec_clone = pairs_to_assets_vec.clone();
            let pair_status_clone = pair_status.clone();
            let public_online_clone = public_online.clone();
            tokio::spawn(async move {
                kraken::fetch_kraken_data_ws(
                    all_pairs_clone,
                    shared_asset_pairs_vec_clone,
                    pairs_to_assets_vec_clone,
                    pair_status_clone,
                    public_online_clone,
                )
                .await
                .expect("Failed to fetch data");
            })
        };

        // Search for arbitrage opportunities, for each graph
        let mut evaluate_handles = Vec::new();
        for i in 0..pairs_to_assets_vec.len() {
            let evaluate_handle = {
                let pair_to_assets_clone = pairs_to_assets_vec[i].clone();
                let shared_asset_pairs_clone = shared_asset_pairs_vec[i].clone();
                let pair_status_clone = pair_status.clone();
                let public_online_clone = public_online.clone();
                tokio::spawn(async move {
                    let _ = evaluate_arbitrage::evaluate_arbitrage_opportunities(
                        pair_to_assets_clone,
                        shared_asset_pairs_clone,
                        pair_status_clone,
                        public_online_clone,
                        i as i64,
                    )
                    .await;
                })
            };
            evaluate_handles.push(evaluate_handle);
        }

        // Wait for tasks to complete (this will likely never happen given the current logic)
        let mut all_handles = vec![Box::pin(fetch_handle)];
        for handle in evaluate_handles {
            all_handles.push(Box::pin(handle));
        }

        // Handle for the private Kraken endpoint
        if allow_trades {
            let private_handle = {
                // let private_stream_clone = Arc::clone(&private_stream);
                let auth_token_clone = Arc::clone(&auth_token);
                tokio::spawn(async move {
                    kraken_private::connect_to_private_feed(auth_token_clone)
                        .await
                        .expect("Failed to connect to private feed");
                })
            };
            all_handles.push(Box::pin(private_handle));
        }

        let (result, index, remaining) = select_all(all_handles).await;

        // Abort all tasks except the one that has already finished or panicked
        for (i, handle) in remaining.into_iter().enumerate() {
            if i != index {
                handle.abort();
            }
        }

        match result {
            Ok(_) => send_telegram_message("Code died: Waiting 10 seconds, then restarting.").await,
            Err(e) => {
                let error_message = format!("A task failed with error: {:?}", e);
                log::info!("{}", error_message);
                send_telegram_message(&error_message).await;
            }
        }
    }
    send_telegram_message("Too many retries: Exiting the program.").await;
    std::process::exit(1);
}
