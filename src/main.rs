use std::sync::{Arc, Mutex};
use std::collections::HashMap;

mod kraken_ws;
mod evaluate_arbitrage;
mod graph_algorithms;

#[tokio::main]
async fn main() {
    let pair_to_assets = kraken_ws::asset_pairs_to_pull().await.expect("Failed to get asset pairs");
    let shared_asset_pairs = Arc::new(Mutex::new(HashMap::new()));

    let fetch_handle = {
        let shared_asset_pairs_clone = shared_asset_pairs.clone();
        let pair_to_assets_clone1 = pair_to_assets.clone();
        tokio::spawn(async move {
            kraken_ws::fetch_kraken_data_ws(pair_to_assets_clone1, shared_asset_pairs_clone).await.expect("Failed to fetch data");
        })
    };

    let evaluate_handle = {
        let shared_asset_pairs_clone = shared_asset_pairs.clone();
        let pair_to_assets_clone2 = pair_to_assets.clone();
        tokio::spawn(async move {
            evaluate_arbitrage::evaluate_arbitrage_opportunities(pair_to_assets_clone2, shared_asset_pairs_clone).await;
        })
    };
    
    // Wait for both tasks to complete (this will likely never happen given the current logic)
    let _ = tokio::try_join!(fetch_handle, evaluate_handle);
}
