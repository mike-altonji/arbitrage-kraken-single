use std::collections::HashMap;

#[derive(Clone)]
pub struct BaseQuote {
    pub base: String,
    pub quote: String,
}
pub type PairToAssets = HashMap<String, BaseQuote>;

#[derive(Clone)]
#[allow(dead_code)]
pub struct BaseQuotePair {
    pub base: String,
    pub quote: String,
    pub pair: String,
}
pub type AssetsToPair = HashMap<(String, String), BaseQuotePair>;

#[derive(Clone)]
pub struct Spread {
    pub bid: f64,
    pub ask: f64,
    pub kraken_ts: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
}
pub type PairToSpread = HashMap<String, Spread>;

#[derive(Clone)]
pub struct Decimals {
    pub volume: usize,
    pub price: usize,
}
pub type PairToDecimals = HashMap<String, Decimals>;

#[derive(Debug, Clone)]
pub struct TradeMin {
    pub ordermin: f64,
    pub costmin: f64,
}
pub type PairToTradeMin = HashMap<String, TradeMin>;

pub struct Edge {
    pub src: usize,
    pub dest: usize,
    pub weight: f64,
}

/// Holds 2 dictionaries: One to convert from Web Socket to REST format, and vice versa
#[derive(Debug, Clone)]
pub struct AssetNameConverter {
    pub ws_to_rest_map: HashMap<String, String>,
    pub rest_to_ws_map: HashMap<String, String>,
}

impl AssetNameConverter {
    // Create a new AssetNameConverter
    pub fn new() -> AssetNameConverter {
        AssetNameConverter {
            ws_to_rest_map: HashMap::new(),
            rest_to_ws_map: HashMap::new(),
        }
    }

    // Insert a pair into the dictionary
    pub fn insert(&mut self, ws: String, rest: String) {
        self.rest_to_ws_map.insert(rest.clone(), ws.clone());
        self.ws_to_rest_map.insert(ws, rest);
    }

    // Find rest by ws
    #[allow(dead_code)]
    pub fn ws_to_rest(&self, ws: &str) -> Option<&String> {
        self.ws_to_rest_map.get(ws)
    }

    // Find ws by rest
    pub fn rest_to_ws(&self, rest: &str) -> Option<&String> {
        self.rest_to_ws_map.get(rest)
    }
}

impl IntoIterator for AssetNameConverter {
    type Item = (String, String);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let mut vec = Vec::new();
        for (ws, rest) in self.ws_to_rest_map {
            vec.push((ws, rest));
        }
        vec.into_iter()
    }
}

#[derive(Debug, Clone)]
pub struct OrderData {
    pub order_id: String,
    pub lastupdated: f64,
    pub vol: f64,
    pub cost: f64,
    pub fee: f64,
    pub price: f64,
}
pub type OrderMap = HashMap<i32, OrderData>;
