use crate::structs::TradeCommand;
use dotenv::dotenv;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicI16};
use tokio::sync::mpsc;

mod arb_forensics;
mod asset_pairs;
mod evaluate_arbitrage;
mod influx;
mod kraken_rest;
mod listener;
mod momentum;
mod orderbook;
mod quote_persist;
mod structs;
mod threads;
mod trade;
mod utils;

// Static atomic variables for fees and balances
pub static FEE_SPOT: AtomicI16 = AtomicI16::new(40); // Default 0.40% (in bps)
pub static FEE_STABLECOIN: AtomicI16 = AtomicI16::new(20); // Default 0.20% (in bps)
pub static USD_BALANCE: AtomicI16 = AtomicI16::new(0);
pub static EUR_BALANCE: AtomicI16 = AtomicI16::new(0);

// Trader busy flag - used to drop orders if trader is processing
pub static TRADER_BUSY: AtomicBool = AtomicBool::new(false);

// Depth-walk risk knobs (see docs/superpowers/specs/2026-07-18-dual-walk-momentum-design.md)
pub static MAX_WALK_DEPTH: AtomicI16 = AtomicI16::new(10); // Max levels consumed per side
pub static DEPTH_HAIRCUT_PCT: AtomicI16 = AtomicI16::new(70); // % of displayed volume planned beyond L0
pub static ROI_BUFFER_BPS: AtomicI16 = AtomicI16::new(2); // Marginal ROI must exceed 1 + buffer

// Momentum trade mode: same-pair round trip triggered by the sibling pair jumping
pub static MOMENTUM_ENABLED: AtomicBool = AtomicBool::new(false);

// Quote persistence: improved BBO must survive N further BBO updates before send (0 = off)
pub static PERSIST_UPDATES: AtomicI16 = AtomicI16::new(0);

/// Application configuration parsed from command-line arguments
struct Config {
    allow_trades: bool,
    debug_mode: bool,
    public_ws_url: String,
    private_ws_url: String,
    token: String,
}

/// Parse `--flag N` style args; returns None when absent or unparseable.
fn parse_arg_value(args: &[String], flag: &str) -> Option<i16> {
    let idx = args.iter().position(|a| a == flag)?;
    let value = args.get(idx + 1)?;
    match value.parse::<i16>() {
        Ok(v) => Some(v),
        Err(_) => {
            log::warn!("Ignoring {}: could not parse '{}' as integer", flag, value);
            None
        }
    }
}

impl Config {
    async fn initialize() -> Self {
        let args: Vec<String> = env::args().collect();
        let use_colocated = args.contains(&"--colocated".to_string());

        if let Some(v) = parse_arg_value(&args, "--max-walk-depth") {
            MAX_WALK_DEPTH.store(v, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(v) = parse_arg_value(&args, "--depth-haircut") {
            DEPTH_HAIRCUT_PCT.store(v, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(v) = parse_arg_value(&args, "--roi-buffer-bps") {
            ROI_BUFFER_BPS.store(v, std::sync::atomic::Ordering::Relaxed);
        }
        if args.contains(&"--momentum".to_string()) {
            MOMENTUM_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(v) = parse_arg_value(&args, "--persist-updates") {
            PERSIST_UPDATES.store(v.max(0), std::sync::atomic::Ordering::Relaxed);
        }
        log::info!(
            "Risk knobs: max_walk_depth={}, depth_haircut_pct={}, roi_buffer_bps={}, momentum={}, persist_updates={}",
            MAX_WALK_DEPTH.load(std::sync::atomic::Ordering::Relaxed),
            DEPTH_HAIRCUT_PCT.load(std::sync::atomic::Ordering::Relaxed),
            ROI_BUFFER_BPS.load(std::sync::atomic::Ordering::Relaxed),
            MOMENTUM_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
            PERSIST_UPDATES.load(std::sync::atomic::Ordering::Relaxed),
        );
        let (public_ws_url, private_ws_url) = if use_colocated {
            (
                "wss://colo-london.vip-ws.kraken.com".to_string(),
                "wss://colo-london.vip-ws-auth.kraken.com".to_string(),
            )
        } else {
            (
                "wss://ws.kraken.com".to_string(),
                "wss://ws-auth.kraken.com".to_string(),
            )
        };

        Self {
            allow_trades: args.contains(&"--trade".to_string()),
            debug_mode: args.contains(&"--debug".to_string()),
            public_ws_url,
            private_ws_url,
            token: utils::get_ws_auth_token()
                .await
                .expect("Could not pull auth token."),
        }
    }
}

/// Initializes the application: logging, environment, and configuration
async fn initialize_app() -> Config {
    dotenv().ok();
    let config = Config::initialize().await;
    utils::init_logging(config.debug_mode);
    arb_forensics::init();

    let mode_message = if config.allow_trades {
        "💰 Launching Kraken arbitrage: Trade mode"
    } else {
        "🚀 Launching Kraken arbitrage: Evaluation-only mode"
    };
    utils::send_telegram_message(mode_message).await;

    config
}

#[tokio::main]
async fn main() {
    // Initialize application
    let config = initialize_app().await;

    // Create bounded channel for sending trade commands to trading thread
    let (trade_tx, trade_rx) = mpsc::channel::<TradeCommand>(1);

    // Get available cores for pinning
    let cores = core_affinity::get_core_ids().expect("Could not get core IDs");

    // Spawn all threads
    let mut handles = Vec::new();

    // Create listener threads
    handles.extend(threads::spawn_listener_threads(
        &cores,
        config.public_ws_url.clone(),
        trade_tx,
    ));

    // Create balance and fee fetcher threads
    handles.push(threads::spawn_balance_fetcher_thread(&cores));
    handles.push(threads::spawn_fee_fetcher_thread(&cores));

    // Create trading thread
    handles.push(threads::spawn_trading_thread(
        &cores,
        config.token.clone(),
        config.private_ws_url.clone(),
        trade_rx,
        config.allow_trades,
    ));

    // Wait for all threads (they run indefinitely)
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}
