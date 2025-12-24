use crate::asset_pairs;
use crate::kraken_rest;
use crate::listener;
use crate::structs::OrderInfo;
use crate::trade;
use crate::utils::{build_pair_names_vec, initialize_pair_data};
use crate::{EUR_BALANCE, FEE_SPOT, FEE_STABLECOIN, USD_BALANCE};
use std::thread;
use tokio::sync::mpsc;

/// Creates a pinned thread with a tokio runtime that runs the provided async function
fn spawn_pinned_thread<F, Fut>(
    cores: &[core_affinity::CoreId],
    core_id: usize,
    thread_name: String,
    f: F,
) -> thread::JoinHandle<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    log::debug!("Spawning thread {} on core {}", thread_name, core_id);
    ensure_core_available(cores, core_id);
    let core_id_obj = core_affinity::CoreId { id: core_id };
    thread::spawn(move || {
        // Pin thread to core
        if core_affinity::set_for_current(core_id_obj) {
            log::debug!("{} pinned to core {}", thread_name, core_id);
        } else {
            #[cfg(target_os = "macos")]
            log::warn!("Thread pinning not supported on macOS. Continuing without pinning");
            #[cfg(not(target_os = "macos"))]
            log::error!("{} failed to pin to core {}", thread_name, core_id);
            panic!("{} failed to pin to core {}", thread_name, core_id);
        }

        // Create a tokio runtime on this thread
        let rt = tokio::runtime::Runtime::new().unwrap_or_else(|e| {
            panic!(
                "Failed to create tokio runtime for {}: {:?}",
                thread_name, e
            )
        });
        rt.block_on(f());
    })
}

/// Verifies that a core is available, panicking if not
fn ensure_core_available(cores: &[core_affinity::CoreId], core_id: usize) {
    if !cores.iter().any(|c| c.id == core_id) {
        log::error!("Core {} not available", core_id);
        panic!("Core {} not available", core_id);
    }
}

/// Creates all listener threads
pub fn spawn_listener_threads(
    cores: &[core_affinity::CoreId],
    public_ws_url: String,
    trade_tx: mpsc::Sender<OrderInfo>,
) -> Vec<thread::JoinHandle<()>> {
    let asset_indices = vec![
        (&asset_pairs::ASSET_INDEX_0, 0),
        (&asset_pairs::ASSET_INDEX_1, 1),
        (&asset_pairs::ASSET_INDEX_2, 2),
        (&asset_pairs::ASSET_INDEX_3, 3),
        (&asset_pairs::ASSET_INDEX_4, 4),
        (&asset_pairs::ASSET_INDEX_5, 5),
    ];

    asset_indices
        .into_iter()
        .map(|(asset_index, thread_id)| {
            let target_core_id = thread_id % 3;
            let trade_tx_clone = trade_tx.clone();
            let public_ws_url_clone = public_ws_url.clone();
            let asset_index_clone = asset_index;

            spawn_pinned_thread(
                cores,
                target_core_id,
                format!("Listener {}", thread_id),
                move || {
                    async move {
                        // Build pair names vector from asset_index
                        // Separate from pair_data_vec to keep pair_data_vec slim to fit on one cache line
                        let pair_names = build_pair_names_vec(asset_index_clone);

                        // Initialize pair data from Kraken API
                        let mut pair_data_vec = initialize_pair_data(asset_index_clone).await;
                        let mut public_online = true;

                        // Run listener
                        listener::run_listening_thread(
                            asset_index_clone,
                            &mut pair_data_vec,
                            &mut public_online,
                            &public_ws_url_clone,
                            &pair_names,
                            trade_tx_clone,
                        )
                        .await
                    }
                },
            )
        })
        .collect()
}

/// Creates the balance fetcher
pub fn spawn_balance_fetcher_thread(cores: &[core_affinity::CoreId]) -> thread::JoinHandle<()> {
    spawn_pinned_thread(cores, 3, "Balance Fetcher".to_string(), || async move {
        kraken_rest::fetch_asset_balances(&USD_BALANCE, &EUR_BALANCE).await;
    })
}

/// Creates the fee fetcher
pub fn spawn_fee_fetcher_thread(cores: &[core_affinity::CoreId]) -> thread::JoinHandle<()> {
    spawn_pinned_thread(cores, 3, "Fee Fetcher".to_string(), || async move {
        kraken_rest::fetch_trading_fees(&FEE_SPOT, &FEE_STABLECOIN).await;
    })
}

/// Creates the trading thread
pub fn spawn_trading_thread(
    cores: &[core_affinity::CoreId],
    token: String,
    private_ws_url: String,
    trade_rx: mpsc::Receiver<OrderInfo>,
    allow_trades: bool,
) -> thread::JoinHandle<()> {
    spawn_pinned_thread(cores, 3, "Trader".to_string(), move || async move {
        trade::run_trading_thread(token, private_ws_url, trade_rx, allow_trades).await;
    })
}
