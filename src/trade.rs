use crate::structs::AssetsToPair;
use std::collections::{HashMap, HashSet};

/// Rotate the cycle-path such that it starts at a `starter`.
/// If multiple starters, do the higher-volatility trade first.
/// If no starters, do nothing.
pub fn rotate_trading_path(
    path: &mut Vec<String>,
    starters: &HashSet<String>,
    asset_pair_volatility: &HashMap<String, f64>,
    assets_to_pair: &AssetsToPair,
) {
    path.pop(); // Pop the final asset, breaking the cycle
    if let Some((index, _)) = path
        .iter()
        .enumerate()
        .filter(|(_, asset)| starters.contains(*asset))
        .map(|(i, asset)| {
            let next_asset = &path[(i + 1) % path.len()];
            let pair = &assets_to_pair[&(asset.clone(), next_asset.clone())].pair;
            let volatility = asset_pair_volatility.get(pair).unwrap_or(&0.0);
            (i, volatility)
        })
        .max_by(|(_, vol1), (_, vol2)| vol1.partial_cmp(vol2).unwrap())
    {
        path.rotate_left(index);
    }

    // Add the first asset back to the end to complete the cycle
    if let Some(first_asset) = path.first() {
        path.push(first_asset.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::BaseQuotePair;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_rotate_trading_path() {
        let starters = HashSet::from(["USD".to_string(), "EUR".to_string()]);

        let assets_to_pair = HashMap::from([
            (
                ("USD".to_string(), "EUR".to_string()),
                BaseQuotePair {
                    base: "EUR".to_string(),
                    quote: "USD".to_string(),
                    pair: "EUR/USD".to_string(),
                },
            ),
            (
                ("EUR".to_string(), "USD".to_string()),
                BaseQuotePair {
                    base: "EUR".to_string(),
                    quote: "USD".to_string(),
                    pair: "EUR/USD".to_string(),
                },
            ),
            (
                ("DOGE".to_string(), "EUR".to_string()),
                BaseQuotePair {
                    base: "DOGE".to_string(),
                    quote: "EUR".to_string(),
                    pair: "DOGE/EUR".to_string(),
                },
            ),
            (
                ("EUR".to_string(), "DOGE".to_string()),
                BaseQuotePair {
                    base: "DOGE".to_string(),
                    quote: "EUR".to_string(),
                    pair: "DOGE/EUR".to_string(),
                },
            ),
            (
                ("USD".to_string(), "DOGE".to_string()),
                BaseQuotePair {
                    base: "DOGE".to_string(),
                    quote: "USD".to_string(),
                    pair: "DOGE/USD".to_string(),
                },
            ),
            (
                ("DOGE".to_string(), "USD".to_string()),
                BaseQuotePair {
                    base: "DOGE".to_string(),
                    quote: "USD".to_string(),
                    pair: "DOGE/USD".to_string(),
                },
            ),
            (
                ("BTC".to_string(), "EUR".to_string()),
                BaseQuotePair {
                    base: "BTC".to_string(),
                    quote: "EUR".to_string(),
                    pair: "BTC/EUR".to_string(),
                },
            ),
            (
                ("EUR".to_string(), "BTC".to_string()),
                BaseQuotePair {
                    base: "BTC".to_string(),
                    quote: "EUR".to_string(),
                    pair: "BTC/EUR".to_string(),
                },
            ),
            (
                ("USD".to_string(), "BTC".to_string()),
                BaseQuotePair {
                    base: "BTC".to_string(),
                    quote: "USD".to_string(),
                    pair: "BTC/USD".to_string(),
                },
            ),
            (
                ("BTC".to_string(), "USD".to_string()),
                BaseQuotePair {
                    base: "BTC".to_string(),
                    quote: "USD".to_string(),
                    pair: "BTC/USD".to_string(),
                },
            ),
        ]);

        let asset_pair_volatility = HashMap::from([
            ("EUR/USD".to_string(), 0.1),
            ("DOGE/EUR".to_string(), 9.9),
            ("DOGE/USD".to_string(), 2.3),
            ("BTC/EUR".to_string(), 0.5),
            ("BTC/USD".to_string(), 0.9),
        ]);

        let mut path1 = vec![
            "USD".to_string(),
            "BTC".to_string(),
            "EUR".to_string(),
            "DOGE".to_string(),
            "USD".to_string(),
        ];

        rotate_trading_path(
            &mut path1,
            &starters,
            &asset_pair_volatility,
            &assets_to_pair,
        );

        let mut path2 = vec!["BTC".to_string(), "DOGE".to_string(), "BTC".to_string()];

        rotate_trading_path(
            &mut path2,
            &starters,
            &asset_pair_volatility,
            &assets_to_pair,
        );

        assert_eq!(
            path1,
            vec![
                "EUR".to_string(),
                "DOGE".to_string(),
                "USD".to_string(),
                "BTC".to_string(),
                "EUR".to_string(),
            ]
        );

        assert_eq!(
            path2,
            vec!["BTC".to_string(), "DOGE".to_string(), "BTC".to_string(),]
        );
    }
}
