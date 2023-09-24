use std::sync::{Arc, Mutex};
use std::collections::HashMap;

// Tests to ensure data is actually updating
pub async fn monitor_btc_usd(asset_pairs: Arc<Mutex<HashMap<String, (f64, f64)>>>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        let pairs = asset_pairs.lock().unwrap();
        // println!("{:?}", pairs);
        if let Some((bid, ask)) = pairs.get("XBT/USD") {
            println!("BTC/USD: Bid: {}, Ask: {}", bid, ask);
        } else {
            println!("BTC/USD data not available yet.");
        }
    }
}