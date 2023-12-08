use std::collections::HashMap;

#[derive(Clone)]
pub struct BaseQuote {
    pub base: String,
    pub quote: String,
}
pub type PairToAssets = HashMap<String, BaseQuote>;

#[derive(Clone)]
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

pub struct Edge {
    pub src: usize,
    pub dest: usize,
    pub weight: f64,
}
