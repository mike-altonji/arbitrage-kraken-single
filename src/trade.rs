use crate::structs::OrderInfo;
use crate::TRADER_BUSY;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

/// Trading thread main loop
/// Receives OrderInfo messages and executes the trading logic
pub async fn run_trading_thread(mut trade_rx: mpsc::Receiver<OrderInfo>) {
    log::debug!("Starting trading thread");
    while let Some(order) = trade_rx.recv().await {
        // Mark trader as busy before processing
        TRADER_BUSY.store(true, Ordering::Relaxed);

        log::debug!("Sending order starting with {}", order.pair1_name);
        make_trades(&order).await;

        // Mark trader as idle after processing
        TRADER_BUSY.store(false, Ordering::Relaxed);
    }
    log::debug!("Trading channel closed, exiting trading thread");
}

async fn make_trades(order: &OrderInfo) {
    let trade_msg1: String;
    trade_msg1 = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "market",
        "volume": format!("{:.*}", order.volume_decimals_coin, order.volume_coin),
        "pair": order.pair1_name,
    })
    .to_string();
}
