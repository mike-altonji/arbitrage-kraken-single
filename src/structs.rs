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
#[allow(dead_code)] // Stablecoin legs unused while we skip trading back to stables
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
    pub updated_pair_kraken_ts: f64,
    /// Links trade execution forensics back to the evaluation that created this order.
    pub opportunity_id: u64,
    pub planned_vwap_ask: f64,
    pub planned_vwap_bid: f64,
    /// `pair1_stable.bid / pair2_stable.ask` at eval time — converts pair2 quote → pair1 quote.
    pub quote2_to_quote1_fx: f64,
}
