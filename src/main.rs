use dotenv::dotenv;
use evaluate_arbitrage::evaluate_arbitrage_opportunities;
use futures::future::select_all;
use influx::spread_latency_from_influx;
use kraken::{fetch_spreads, update_volatility};
use kraken_assets_and_pairs::{extract_asset_pairs_from_csv_files, get_unique_pairs};
use kraken_orders_listener::fetch_orders;
use kraken_private::get_auth_token;
use kraken_private_rest::fetch_asset_balances;
use std::collections::HashMap;
use std::env;
use std::f64::INFINITY;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use structs::{OrderMap, PairToVolatility};
use tokio::time::sleep;

mod evaluate_arbitrage;
mod graph_algorithms;
mod influx;
mod kraken;
mod kraken_assets_and_pairs;
mod kraken_orders_listener;
mod kraken_private;
mod kraken_private_rest;
mod structs;
mod telegram;
mod trade;
mod utils;

use crate::kraken::update_fees_based_on_volume;
use crate::kraken_private::get_30d_trade_volume;
use crate::telegram::send_telegram_message;

#[tokio::main]
async fn main() {
    // Initialize setup
    dotenv().ok();
    let args: Vec<String> = env::args().collect();
    let allow_trades = args.contains(&"--trade".to_string());
    utils::init_logging();
    let mode_message = if allow_trades {
        "🚀 Launching Kraken arbitrage: Trade mode"
    } else {
        "🚀 Launching Kraken arbitrage: Evaluation-only mode"
    };
    send_telegram_message(mode_message).await;
    let token = if allow_trades {
        Some(get_auth_token().await.expect("Could not pull auth token."))
    } else {
        None
    };

    // Loop allows retries
    let mut retry = 0;
    while retry <= 5 {
        retry += 1;
        sleep(Duration::from_secs(10)).await;

        // Variables initialized for later use
        let pair_status: Arc<Mutex<HashMap<String, bool>>> = Arc::new(Mutex::new(HashMap::new()));
        let public_online = Arc::new(Mutex::new(false));
        let p90_latency = Arc::new(Mutex::new(INFINITY));

        // Pull asset pairs and initialize bids/asks
        let (
            pair_to_assets_vec,
            assets_to_pair_vec,
            pair_to_spread_vec,
            all_fee_schedules,
            all_asset_pair_conversion,
            all_asset_name_conversion,
        ) = extract_asset_pairs_from_csv_files("resources")
            .await
            .expect("Failed to get asset pairs");

        // Keep bids/asks up to date
        let fetch_handle = {
            let all_pairs = get_unique_pairs(&pair_to_assets_vec);
            let pair_to_spread_vec_clone = pair_to_spread_vec.clone();
            let pair_to_assets_vec_clone = pair_to_assets_vec.clone();
            let pair_status_clone = pair_status.clone();
            let public_online_clone = public_online.clone();
            tokio::spawn(async move {
                fetch_spreads(
                    all_pairs,
                    pair_to_spread_vec_clone,
                    pair_to_assets_vec_clone,
                    pair_status_clone,
                    public_online_clone,
                )
                .await
                .expect("Failed to fetch data");
            })
        };
        let mut all_handles = vec![Box::pin(fetch_handle)];

        // Keep orders up to date
        let orders = Arc::new(Mutex::new(OrderMap::new()));
        if allow_trades {
            let token_clone = token.clone().expect("Token must exist to query orders");
            let orders_handle = {
                let orders_clone = orders.clone();
                tokio::spawn(async move {
                    fetch_orders(&token_clone, &orders_clone)
                        .await
                        .expect("Failed to fetch data");
                })
            };
            all_handles.push(Box::pin(orders_handle));
        }

        // Keep balances up to date
        let balances = Arc::new(Mutex::new(HashMap::<String, f64>::new()));
        if allow_trades {
            let balance_handle = {
                let balances_clone = balances.clone();
                tokio::spawn(async move {
                    fetch_asset_balances(&balances_clone, &all_asset_name_conversion)
                        .await
                        .expect("Failed to fetch data balances");
                })
            };
            all_handles.push(Box::pin(balance_handle));
        }

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
                            log::warn!("Unable to fetch 30 day trading volume: Defaulting to 0.");
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
        all_handles.push(Box::pin(fees_handle));

        // Task dedicated to keeping volatility up to date
        let volatility: Arc<Mutex<PairToVolatility>> = Arc::new(Mutex::new(
            all_asset_pair_conversion
                .ws_to_rest_map
                .keys()
                .map(|key| (key.clone(), INFINITY))
                .collect(),
        ));
        let volatility_clone = volatility.clone();
        let volatility_handle = {
            let asset_pair_conversion = all_asset_pair_conversion.clone();
            tokio::spawn(async move {
                loop {
                    {
                        let volatility_clone2 = volatility.clone();
                        update_volatility(volatility_clone2, &asset_pair_conversion)
                            .await
                            .expect("Volatility pull failed");
                    }
                    sleep(Duration::from_secs(10)).await;
                }
            })
        };
        all_handles.push(Box::pin(volatility_handle));

        // Task dedicated to knowing the p90 latency of spread fetches
        let latency_handle = {
            let p90_latency_clone = p90_latency.clone();
            tokio::spawn(async move {
                loop {
                    spread_latency_from_influx(p90_latency_clone.clone())
                        .await
                        .expect("Failed to fetch latency from InfluxDB.");
                    sleep(Duration::from_secs(5)).await;
                }
            })
        };
        all_handles.push(Box::pin(latency_handle));

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
                let token_clone = token.clone();
                let p90_latency_clone = p90_latency.clone();
                let volatility_clone = volatility_clone.clone();
                let orders_clone = orders.clone();
                let balances_clone = balances.clone();
                tokio::spawn(async move {
                    let _ = evaluate_arbitrage_opportunities(
                        pair_to_assets_clone,
                        assets_to_pair_clone,
                        pair_to_spread_clone,
                        fees_clone,
                        pair_status_clone,
                        public_online_clone,
                        p90_latency_clone,
                        allow_trades,
                        token_clone.as_deref(),
                        i as i64,
                        volatility_clone,
                        orders_clone,
                        balances_clone,
                    )
                    .await;
                })
            };
            evaluate_handles.push(evaluate_handle);
        }
        for handle in evaluate_handles {
            all_handles.push(Box::pin(handle));
        }

        let (result, _index, remaining) = select_all(all_handles).await;
        match result {
            Ok(_) => send_telegram_message("Code died: Waiting 10 seconds, then restarting.").await,
            Err(_e) => {
                let message = format!("Join error - Retry # {retry}");
                log::error!("{}", message);
                send_telegram_message(&message).await;
            }
        }

        // Abort tasks upon failure or completion before restarting
        for (_i, handle) in remaining.into_iter().enumerate() {
            handle.abort();
        }
    }
    send_telegram_message("Too many retries: Exiting the program.").await;
    std::process::exit(1);
}
