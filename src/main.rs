use dotenv::dotenv;
use futures::future::select_all;
use kraken_private::get_auth_token;
use log4rs::{append::file::FileAppender, config};
use std::collections::{HashMap, HashSet};
use std::env;
use std::f64::INFINITY;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

mod evaluate_arbitrage;
mod graph_algorithms;
mod influx;
mod kraken;
mod kraken_private;
mod telegram;
mod trade;

use crate::kraken::update_fees_based_on_volume;
use crate::kraken_private::get_30d_trade_volume;
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
    let token = if allow_trades {
        get_auth_token().await.expect("Could not pull auth token.")
    } else {
        "".to_string()
    };

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
        let mut pair_to_assets_vec = Vec::new();
        let mut assets_to_pair_vec = Vec::new();
        let mut pair_to_spread_vec = Vec::new();
        let pair_status: Arc<Mutex<HashMap<String, bool>>> = Arc::new(Mutex::new(HashMap::new()));
        let public_online = Arc::new(Mutex::new(false));
        let p90_latency = Arc::new(Mutex::new(INFINITY));
        let mut all_fee_schedules = HashMap::new();

        for csv_file in csv_files {
            let (pair_to_assets, assets_to_pair, fee_schedules) =
                kraken::asset_pairs_to_pull(&csv_file)
                    .await
                    .expect("Failed to get asset pairs");
            let pair_to_spread = Arc::new(Mutex::new(HashMap::new()));
            pair_to_assets_vec.push(pair_to_assets);
            assets_to_pair_vec.push(assets_to_pair);
            pair_to_spread_vec.push(pair_to_spread);
            for (key, value) in fee_schedules {
                all_fee_schedules.insert(key, value);
            }
        }

        // Unique set of all pairs, so we just have 1 subscription to the Kraken websocket
        let mut all_pairs = HashSet::new();
        for pair_to_assets in &pair_to_assets_vec {
            for pair in pair_to_assets.keys() {
                all_pairs.insert(pair.clone());
            }
        }

        // Keep bids/asks up to date
        let fetch_handle = {
            let all_pairs_clone = all_pairs.clone();
            let pair_to_spread_vec_clone = pair_to_spread_vec.clone();
            let pair_to_assets_vec_clone = pair_to_assets_vec.clone();
            let pair_status_clone = pair_status.clone();
            let public_online_clone = public_online.clone();
            tokio::spawn(async move {
                kraken::fetch_spreads(
                    all_pairs_clone,
                    pair_to_spread_vec_clone,
                    pair_to_assets_vec_clone,
                    pair_status_clone,
                    public_online_clone,
                )
                .await
                .expect("Failed to fetch data");
            })
        };

        // Task dedicated to grabbing the most recent fee
        let fees: Arc<Mutex<HashMap<String, f64>>> = Arc::new(Mutex::new(
            all_fee_schedules
                .keys()
                .map(|key| (key.clone(), 0.0026))
                .collect(),
        ));
        let fees_handle = {
            let fees_clone = fees.clone();
            let schedules_clone = all_fee_schedules.clone();
            tokio::spawn(async move {
                loop {
                    let vol = match get_30d_trade_volume().await {
                        Ok(volume) => volume,
                        Err(_) => {
                            log::error!("Error: Defaulting volume to 0.0");
                            0.0
                        }
                    };
                    // Lock the mutex only when updating the fees
                    {
                        let mut fees = fees_clone.lock().unwrap();
                        update_fees_based_on_volume(&mut *fees, &schedules_clone, vol);
                    }
                    sleep(Duration::from_secs(10)).await;
                }
            })
        };

        // Task dedicated to knowing the p90 latency of spread fetches
        let latency_handle = {
            let p90_latency_clone = p90_latency.clone();
            tokio::spawn(async move {
                loop {
                    influx::spread_latency_from_influx(p90_latency_clone.clone())
                        .await
                        .expect("Failed to fetch latency from InfluxDB.");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            })
        };

        // Search for arbitrage opportunities, for each graph
        let mut evaluate_handles = Vec::new();
        for i in 0..pair_to_assets_vec.len() {
            let evaluate_handle = {
                let pair_to_assets_clone = pair_to_assets_vec[i].clone();
                let assets_to_pair_clone = assets_to_pair_vec[i].clone();
                let pair_to_spread_clone = pair_to_spread_vec[i].clone();
                let fees_clone = fees.clone();
                let pair_status_clone = pair_status.clone();
                let public_online_clone = public_online.clone();
                let token = token.clone();
                let p90_latency_clone = p90_latency.clone();
                tokio::spawn(async move {
                    let _ = evaluate_arbitrage::evaluate_arbitrage_opportunities(
                        pair_to_assets_clone,
                        assets_to_pair_clone,
                        pair_to_spread_clone,
                        fees_clone,
                        pair_status_clone,
                        public_online_clone,
                        p90_latency_clone,
                        allow_trades,
                        token.as_str(),
                        i as i64,
                    )
                    .await;
                })
            };
            evaluate_handles.push(evaluate_handle);
        }

        // Wait for both tasks to complete (this will likely never happen given the current logic)
        let mut all_handles = vec![Box::pin(fetch_handle)];
        all_handles.push(Box::pin(fees_handle));
        all_handles.push(Box::pin(latency_handle));
        for handle in evaluate_handles {
            all_handles.push(Box::pin(handle));
        }

        let (result, _index, remaining) = select_all(all_handles).await;

        // Abort tasks upon failure or completion
        for (_i, handle) in remaining.into_iter().enumerate() {
            handle.abort();
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
