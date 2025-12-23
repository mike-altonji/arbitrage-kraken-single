use dotenv::dotenv;
use std::env;
use std::sync::atomic::{AtomicI16, AtomicU16};
use std::thread;
use telegram::send_telegram_message;

mod asset_pairs;
mod evaluate_arbitrage;
mod kraken_rest;
mod listener;
mod structs;
mod telegram;
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

    // Get available cores for pinning
    let cores = core_affinity::get_core_ids().expect("Could not get core IDs");

    // Create 6 listener threads
    let asset_indices = vec![
        (&asset_pairs::ASSET_INDEX_0, 0),
        (&asset_pairs::ASSET_INDEX_1, 1),
        (&asset_pairs::ASSET_INDEX_2, 2),
        (&asset_pairs::ASSET_INDEX_3, 3),
        (&asset_pairs::ASSET_INDEX_4, 4),
        (&asset_pairs::ASSET_INDEX_5, 5),
    ];

    let mut handles = Vec::new();

    for (asset_index, thread_id) in asset_indices {
        // Determine which core to pin to (cycling)
        let target_core_id = thread_id % 3;
        let core_available = cores.iter().any(|c| c.id == target_core_id);
        if !core_available {
            panic!("Core {} not available", target_core_id)
        };
        let core_id = core_affinity::CoreId { id: target_core_id };
        let handle = thread::spawn(move || {
            // Pin thread to core
            if core_affinity::set_for_current(core_id) {
                log::debug!("Thread {} pinned to core {}", thread_id, core_id.id);
            } else {
                #[cfg(target_os = "macos")]
                log::warn!("Thread pinning not supported on macOS. Continuing without pinning");
                #[cfg(not(target_os = "macos"))]
                panic!("Thread {} failed to pin to core {}", thread_id, core_id.id);
            }

            // Create a tokio runtime on this thread
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            rt.block_on(async move {
                log::debug!("Initializing listener thread {}", thread_id);

                // Initialize pair data from Kraken API
                let mut pair_data_vec = utils::initialize_pair_data(asset_index).await;
                let mut public_online = true;

                // Start listener
                let public_ws_url_clone = public_ws_url.to_string();
                listener::start_listener(
                    asset_index,
                    &mut pair_data_vec,
                    &mut public_online,
                    &public_ws_url_clone,
                )
                .await
            });
        });

        handles.push(handle);
    }

    // Create balance fetcher thread (pinned to core 3)
    let core_3_available = cores.iter().any(|c| c.id == 3);
    if !core_3_available {
        panic!("Core 3 not available");
    }
    let core_3_id = core_affinity::CoreId { id: 3 };
    let usd_balance = AtomicI16::new(0);
    let eur_balance = AtomicI16::new(0);
    let balance_handle = thread::spawn(move || {
        // Pin thread to core 3
        if core_affinity::set_for_current(core_3_id) {
            log::debug!("Balance fetcher thread pinned to core 3");
        } else {
            #[cfg(target_os = "macos")]
            log::warn!("Thread pinning not supported on macOS. Continuing without pinning");
            #[cfg(not(target_os = "macos"))]
            panic!("Balance fetcher thread failed to pin to core 3");
        }

        // Create a tokio runtime on this thread
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        rt.block_on(async move {
            log::debug!("Starting balance fetcher thread");
            if let Err(e) = kraken_rest::fetch_asset_balances(&usd_balance, &eur_balance).await {
                log::error!("Balance fetcher error: {:?}", e);
            }
        });
    });

    // Create fee fetcher thread (pinned to core 3)
    let fee_spot = AtomicU16::new(40); // Default 0.40% (formatted in bps as 40)
    let fee_stablecoin = AtomicU16::new(20); // Default 0.20% (formatted in bps as 20)
    let fee_handle = thread::spawn(move || {
        // Pin thread to core 3
        if core_affinity::set_for_current(core_3_id) {
            log::debug!("Fee fetcher thread pinned to core 3");
        } else {
            #[cfg(target_os = "macos")]
            log::warn!("Thread pinning not supported on macOS. Continuing without pinning");
            #[cfg(not(target_os = "macos"))]
            panic!("Fee fetcher thread failed to pin to core 3");
        }

        // Create a tokio runtime on this thread
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        rt.block_on(async move {
            log::debug!("Starting fee fetcher thread");
            if let Err(e) = kraken_rest::fetch_trading_fees(&fee_spot, &fee_stablecoin).await {
                log::error!("Fee fetcher error: {:?}", e);
            }
        });
    });

    // Wait for all threads (they run indefinitely)
    handles.push(balance_handle);
    handles.push(fee_handle);
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}
