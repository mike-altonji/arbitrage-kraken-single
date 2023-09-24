mod kraken_ws;

#[tokio::main]
async fn main() {
    let pair_to_assets = kraken_ws::asset_pairs_to_pull().await.expect("Failed to get asset pairs");
    let asset_pairs = kraken_ws::fetch_kraken_data_ws(pair_to_assets.clone()).await.expect("Failed to fetch data");

    println!("Asset Pairs:");
    for (pair, (bid, ask)) in &asset_pairs {
        println!("{}: Bid: {}, Ask: {}", pair, bid, ask);
    }

    println!("\nPair to Assets:");
    for (pair, (base, quote)) in &pair_to_assets {
        println!("{}: Base: {}, Quote: {}", pair, base, quote);
    }
}