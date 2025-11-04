use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{
    kraken::asset_pairs_to_pull,
    structs::{
        AssetNameConverter, AssetsToPair, PairToAssets, PairToDecimals, PairToSpread,
        PairToTradeMin,
    },
};

pub async fn extract_asset_pairs_from_csv_file(
    csv_file: &str,
) -> Result<
    (
        PairToAssets,
        AssetsToPair,
        Arc<Mutex<PairToSpread>>,
        HashMap<String, Vec<Vec<f64>>>,
        PairToDecimals,
        PairToTradeMin,
        AssetNameConverter,
    ),
    Box<dyn std::error::Error>,
> {
    let csv_file = csv_file.to_string();
    let pair_to_spread = Arc::new(Mutex::new(HashMap::new()));
    let (
        pair_to_assets,
        assets_to_pair,
        asset_name_conversion,
        fee_schedules,
        pair_to_decimals,
        pair_trade_mins,
    ) = asset_pairs_to_pull(&csv_file).await?;

    Ok((
        pair_to_assets,
        assets_to_pair,
        pair_to_spread,
        fee_schedules,
        pair_to_decimals,
        pair_trade_mins,
        asset_name_conversion,
    ))
}

pub fn get_unique_pairs(pair_to_assets: &PairToAssets) -> HashSet<String> {
    let mut all_pairs = HashSet::new();
    for pair in pair_to_assets.keys() {
        all_pairs.insert(pair.clone());
    }
    all_pairs
}
