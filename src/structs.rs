/// Trade execution mode
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TradeMode {
    Market,   // Market orders with 1ms delay between trades
    LimitIoc, // LIMIT IOC buy, listen to ownTrades, then market sell
}

#[derive(Clone)]
pub struct PairData {
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub order_min: f64,
    pub cost_min: f64,
    pub price_decimals: usize,
    pub volume_decimals: usize,
    pub pair_status: bool,
    pub kraken_ts: f64,
}
pub type PairDataVec = Vec<PairData>;

#[derive(Clone)]
#[allow(dead_code)] // Removed stablecoin trading for now, so these fields are unused
pub struct OrderInfo {
    pub pair1_name: &'static str,
    pub pair2_name: &'static str,
    pub pair1_stable_name: &'static str,
    pub pair2_stable_name: &'static str,
    pub volume_coin: f64,
    pub volume_stable: f64,
    pub volume_decimals_coin: usize,
    pub volume_decimals_stable: usize,
    pub send_timestamp: u128,
    pub pair1_price: f64,
    pub price_decimals: usize,
}
