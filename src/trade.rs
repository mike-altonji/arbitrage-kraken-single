use crate::structs::BuyOrder;
use crate::TRADER_BUSY;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

/// Trading thread main loop
/// Receives BuyOrder messages and executes the trading logic
pub async fn run_trading_thread(mut trade_rx: mpsc::Receiver<BuyOrder>) {
    log::debug!("Starting trading thread");
    while let Some(buy_order) = trade_rx.recv().await {
        // Mark trader as busy before processing
        TRADER_BUSY.store(true, Ordering::Relaxed);

        println!(
            "Sending buy order: {} {} {}",
            buy_order.pair_name, buy_order.volume, buy_order.price
        );
        // TODO: Implement actual trading logic here

        // Mark trader as idle after processing
        TRADER_BUSY.store(false, Ordering::Relaxed);
    }
    log::debug!("Trading channel closed, exiting trading thread");
}
