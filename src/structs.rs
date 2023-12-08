#[derive(Clone)]
pub struct PairToAssets {
    pub base: String,
    pub quote: String,
}

#[derive(Clone)]
pub struct AssetsToPair {
    pub base: String,
    pub quote: String,
    pub pair: String,
}

#[derive(Clone)]
pub struct Spread {
    pub bid: f64,
    pub ask: f64,
    pub kraken_ts: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
}

pub struct Edge {
    pub src: usize,
    pub dest: usize,
    pub weight: f64,
}
