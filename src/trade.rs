use crate::structs::OrderInfo;
use crate::TRADER_BUSY;
use std::sync::atomic::Ordering;
use std::time::Duration;
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
    let trade_msg: String;
    let vol_coin_formatted = format!("{:.*}", order.volume_decimals_coin, order.volume_coin);
    trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "market",
        "volume": vol_coin_formatted,
        "pair": order.pair1_name,
    })
    .to_string();
    // TODO: TRADE
    tokio::time::sleep(Duration::from_millis(1)).await; // TODO: Make this 1ms in a better way soon

    trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "sell",
        "ordertype": "market",
        "volume": vol_coin_formatted,
        "pair": order.pair2_name,
    })
    .to_string();
    // TODO: TRADE
    tokio::time::sleep(Duration::from_millis(1)).await; // TODO: Make this 1ms in a better way soon

    let vol_stable_formatted = format!("{:.*}", order.volume_decimals_stable, order.volume_stable);
    trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "market",
        "volume": vol_stable_formatted,
        "pair": order.pair2_stable_name,
    })
    .to_string();
    // TODO: TRADE
    tokio::time::sleep(Duration::from_millis(1)).await; // TODO: Make this 1ms in a better way soon

    trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "sell",
        "ordertype": "market",
        "volume": vol_stable_formatted,
        "pair": order.pair1_stable_name,
    })
    .to_string();
    // TODO: TRADE
}
