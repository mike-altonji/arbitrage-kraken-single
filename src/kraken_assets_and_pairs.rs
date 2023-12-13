use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{
    kraken::asset_pairs_to_pull,
    structs::{AssetNameConverter, AssetsToPair, PairToAssets, PairToSpread, PairToTradeMin},
    utils::get_csv_files_from_directory,
};

pub async fn extract_asset_pairs_from_csv_files(
    directory: &str,
) -> Result<
    (
        Vec<PairToAssets>,
        Vec<AssetsToPair>,
        Vec<Arc<Mutex<PairToSpread>>>,
        HashMap<String, Vec<Vec<f64>>>,
        PairToTradeMin,
        AssetNameConverter,
        AssetNameConverter,
    ),
    Box<dyn std::error::Error>,
> {
    let mut pair_to_assets_vec = Vec::new();
    let mut assets_to_pair_vec = Vec::new();
    let mut pair_to_spread_vec = Vec::new();
    let mut all_fee_schedules = HashMap::new();
    let mut all_pair_trade_mins = PairToTradeMin::new();
    let mut all_asset_pair_conversion = AssetNameConverter::new();
    let mut all_asset_name_conversion = AssetNameConverter::new();

    let csv_files = get_csv_files_from_directory(directory).expect("Failed to read directory");
    for csv_file in csv_files {
        let (
            pair_to_assets,
            assets_to_pair,
            asset_name_conversion,
            asset_pair_conversion,
            fee_schedules,
            pair_trade_mins,
        ) = asset_pairs_to_pull(&csv_file).await?;
        let pair_to_spread = Arc::new(Mutex::new(HashMap::new()));
        pair_to_assets_vec.push(pair_to_assets);
        assets_to_pair_vec.push(assets_to_pair);
        pair_to_spread_vec.push(pair_to_spread);
        for (key, value) in fee_schedules {
            all_fee_schedules.insert(key, value);
        }
        for (key, value) in pair_trade_mins {
            all_pair_trade_mins.insert(key, value);
        }
        for (ws, rest) in asset_pair_conversion {
            all_asset_pair_conversion.insert(ws, rest);
        }
        all_asset_name_conversion = asset_name_conversion; // Overwrite, since all the same
    }

    Ok((
        pair_to_assets_vec,
        assets_to_pair_vec,
        pair_to_spread_vec,
        all_fee_schedules,
        all_pair_trade_mins,
        all_asset_pair_conversion,
        all_asset_name_conversion,
    ))
}

pub fn get_unique_pairs(pair_to_assets_vec: &Vec<PairToAssets>) -> HashSet<String> {
    let mut all_pairs = HashSet::new();
    for pair_to_assets in pair_to_assets_vec {
        for pair in pair_to_assets.keys() {
            all_pairs.insert(pair.clone());
        }
    }
    all_pairs
}
