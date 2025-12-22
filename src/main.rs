use dotenv::dotenv;
use evaluate_arbitrage::evaluate_arbitrage_opportunities;
use futures::future::select_all;
use kraken::{fetch_spreads, update_fees_based_on_volume};
use kraken_assets_and_pairs::{extract_asset_pairs_from_csv_file, get_unique_pairs};
use kraken_orders_listener::fetch_orders;
use kraken_private::{get_30d_trade_volume, get_auth_token, setup_own_trades_websocket};
use kraken_private_rest::fetch_asset_balances;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use structs::OrderMap;
use telegram::send_telegram_message;
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

#[tokio::main]
async fn main() {
    // Initialize setup
    dotenv().ok();
    let args: Vec<String> = env::args().collect();
    let allow_trades = args.contains(&"--trade".to_string());
    let use_colocated = args.contains(&"--colocated".to_string());

    // Determine WebSocket URLs based on --colocated flag
    let public_ws_url = if use_colocated {
        "wss://colo-london.vip-ws.kraken.com"
    } else {
        "wss://ws.kraken.com"
    };
    let private_ws_url = if use_colocated {
        "wss://colo-london.vip-ws-auth.kraken.com"
    } else {
        "wss://ws-auth.kraken.com"
    };

    utils::init_logging();
    let mode_message = if allow_trades {
        "💰 Launching Kraken arbitrage: Trade mode"
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

        // Pull asset pairs and initialize bids/asks
        let (
            pair_to_assets,
            assets_to_pair,
            pair_to_spread,
            fee_schedules,
            pair_to_decimals,
            pair_trade_mins,
            asset_name_conversion,
        ) = extract_asset_pairs_from_csv_file("resources/asset_pairs_all.csv")
            .await
            .expect("Failed to get asset pairs");

        // Keep bids/asks up to date
        let pair_status: Arc<Mutex<HashMap<String, bool>>> = Arc::new(Mutex::new(HashMap::new()));
        let public_online = Arc::new(Mutex::new(false));
        let fetch_handle = {
            let all_pairs = get_unique_pairs(&pair_to_assets);
            let pair_to_spread_clone = pair_to_spread.clone();
            let pair_to_assets_clone = pair_to_assets.clone();
            let pair_status_clone = pair_status.clone();
            let public_online_clone = public_online.clone();
            tokio::spawn(async move {
                fetch_spreads(
                    all_pairs,
                    pair_to_spread_clone,
                    pair_to_assets_clone,
                    pair_status_clone,
                    public_online_clone,
                    public_ws_url,
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
                    fetch_orders(&token_clone, &orders_clone, private_ws_url)
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
                    fetch_asset_balances(&balances_clone, &asset_name_conversion)
                        .await
                        .expect("Failed to fetch data balances");
                })
            };
            all_handles.push(Box::pin(balance_handle));
        }

        // Task dedicated to grabbing the most recent fee
        let fees: Arc<Mutex<HashMap<String, f64>>> = Arc::new(Mutex::new(
            fee_schedules
                .keys()
                .map(|key| (key.clone(), 0.004))
                .collect(),
        ));
        let fees_handle = {
            let fees_clone = fees.clone();
            let schedules_clone = fee_schedules.clone();
            sleep(Duration::from_secs(3)).await; // Avoids duplicating tokens, which fails
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

        // Get runtime handle for spawning async tasks from sync threads
        let rt_handle = tokio::runtime::Handle::current();

        // Create trade semaphore to limit concurrent trades
        let trade_semaphore = Arc::new(tokio::sync::Semaphore::new(1));

        // Set up websocket connection to private Kraken endpoint if trading
        let own_trades_ws = if allow_trades {
            let ws = setup_own_trades_websocket(
                token.as_ref().expect("Token must exist for trading"),
                private_ws_url,
            )
            .await
            .expect("Failed to set up private WebSocket");
            Some(ws)
        } else {
            None
        };

        // Give pair_to_spread time to populate
        sleep(Duration::from_secs(5)).await;

        // Search for arbitrage opportunities in a single thread (one graph)
        let pair_to_assets_clone = pair_to_assets.clone();
        let assets_to_pair_clone = assets_to_pair.clone();
        let pair_to_spread_clone = pair_to_spread.clone();
        let fees_clone = fees.clone();
        let pair_to_decimals_clone = pair_to_decimals.clone();
        let pair_status_clone = pair_status.clone();
        let public_online_clone = public_online.clone();
        let token_clone = token.clone();
        let orders_clone = orders.clone();
        let balances_clone = balances.clone();
        let pair_trade_mins_clone = pair_trade_mins.clone();
        let own_trades_ws_clone = own_trades_ws.clone();
        let rt_handle_clone = rt_handle.clone();
        let trade_semaphore_clone = trade_semaphore.clone();

        std::thread::spawn(move || {
            let _ = evaluate_arbitrage_opportunities(
                pair_to_assets_clone,
                assets_to_pair_clone,
                pair_to_spread_clone,
                fees_clone,
                pair_to_decimals_clone,
                pair_status_clone,
                public_online_clone,
                allow_trades,
                token_clone,
                0 as i64,
                orders_clone,
                balances_clone,
                pair_trade_mins_clone,
                own_trades_ws_clone,
                rt_handle_clone,
                trade_semaphore_clone,
            );
        });

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
