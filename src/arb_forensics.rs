//! Non-blocking arbitrage forensics logger.
//!
//! Hot path only `try_send`s events onto a bounded channel. A single unpinned
//! writer thread serializes JSON and appends to disk. Telemetry must never
//! block trading.

use serde::Serialize;
use std::fs::{create_dir_all, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::OnceLock;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const CHANNEL_BOUND: usize = 2048;

static SENDER: OnceLock<SyncSender<ForensicsEvent>> = OnceLock::new();
static OPPORTUNITY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct PlannedSlice {
    pub leg: &'static str,
    pub level_idx: u8,
    pub price: f64,
    pub book_volume: f64,
    pub planned_volume: f64,
    pub roi_at_level: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct FillRecord {
    pub volume: f64,
    pub price: f64,
    pub fee: f64,
    pub cost: f64,
    pub side: &'static str,
    pub time: f64,
}

#[derive(Debug)]
pub enum ForensicsEvent {
    ArbOpportunity(ArbOpportunityEvent),
    ArbExecution(ArbExecutionEvent),
}

#[derive(Clone, Debug, Serialize)]
pub struct ArbOpportunityEvent {
    pub event: &'static str,
    pub opportunity_id: u64,
    pub walk_mode: &'static str,
    pub pair1: &'static str,
    pub pair2: &'static str,
    pub bbo_roi: f64,
    pub blended_roi: f64,
    pub depth_volume: f64,
    pub vwap_ask: f64,
    pub vwap_bid: f64,
    pub limit_buy_price: f64,
    pub expected_cost: f64,
    pub expected_proceeds: f64,
    pub expected_pnl: f64,
    pub expected_pnl_bps: f64,
    pub stop_reason: &'static str,
    pub balance_limited: bool,
    pub decision: &'static str,
    pub kraken_ts: f64,
    pub eval_ts_ns: u128,
    pub slices: Vec<PlannedSlice>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArbExecutionEvent {
    pub event: &'static str,
    pub opportunity_id: u64,
    pub userref: i32,
    pub pair1: &'static str,
    pub pair2: &'static str,
    pub requested_volume: f64,
    pub limit_buy_price: f64,
    pub planned_vwap_ask: f64,
    pub planned_vwap_bid: f64,
    pub buy_fills: Vec<FillRecord>,
    pub sell_fills: Vec<FillRecord>,
    pub actual_buy_volume: f64,
    pub actual_buy_vwap: f64,
    pub actual_buy_fee: f64,
    pub actual_sell_volume: f64,
    pub actual_sell_vwap: f64,
    pub actual_sell_fee: f64,
    pub volume_shortfall: f64,
    pub buy_slippage_bps: f64,
    pub sell_slippage_bps: f64,
    pub realized_pnl: f64,
    pub outcome: &'static str,
}

/// Start the forensics writer. Safe to call once at process startup.
pub fn init() {
    let _ = SENDER.get_or_init(|| {
        let (tx, rx) = sync_channel::<ForensicsEvent>(CHANNEL_BOUND);
        let path = default_log_path();
        thread::Builder::new()
            .name("arb-forensics".into())
            .spawn(move || writer_loop(rx, path))
            .expect("Failed to spawn arb-forensics writer thread");
        tx
    });
}

pub fn next_opportunity_id() -> u64 {
    OPPORTUNITY_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn try_log(event: ForensicsEvent) {
    let Some(tx) = SENDER.get() else {
        return;
    };
    match tx.try_send(event) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
            // Telemetry is best-effort; never block the trading path.
        }
    }
}

fn default_log_path() -> PathBuf {
    let _ = create_dir_all("logs");
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    PathBuf::from(format!("logs/arb_events_{}.jsonl", ts))
}

fn writer_loop(rx: std::sync::mpsc::Receiver<ForensicsEvent>, path: PathBuf) {
    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("arb_forensics: failed to open {:?}: {}", path, e);
            // Drain forever so senders don't block on a full channel forever.
            while rx.recv().is_ok() {}
            return;
        }
    };
    log::info!("arb_forensics: writing to {:?}", path);
    let mut writer = BufWriter::new(file);
    let mut write_errors = 0u64;

    while let Ok(event) = rx.recv() {
        let json_result = match &event {
            ForensicsEvent::ArbOpportunity(e) => serde_json::to_string(e),
            ForensicsEvent::ArbExecution(e) => serde_json::to_string(e),
        };
        match json_result {
            Ok(line) => {
                if let Err(e) = writeln!(writer, "{}", line) {
                    write_errors += 1;
                    if write_errors.is_multiple_of(100) {
                        log::error!("arb_forensics: write failed (count={}): {}", write_errors, e);
                    }
                } else if write_errors == 0 {
                    // Periodically flush so eval-only runs see data promptly.
                    let _ = writer.flush();
                }
            }
            Err(e) => {
                write_errors += 1;
                if write_errors.is_multiple_of(100) {
                    log::error!(
                        "arb_forensics: serialize failed (count={}): {}",
                        write_errors,
                        e
                    );
                }
            }
        }
    }
    let _ = writer.flush();
}
