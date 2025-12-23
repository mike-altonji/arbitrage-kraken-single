#[derive(Clone)]
pub struct PairData {
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub order_min: f64,
    pub cost_min: f64,
    pub price_decimals: u8,
    pub volume_decimals: u8,
    pub pair_status: bool,
}
pub type PairDataVec = Vec<PairData>;

#[derive(Clone)]
pub struct OrderInfo {
    pub pair1_name: &'static str,
    pub pair2_name: &'static str,
    pub pair1_stable_name: &'static str,
    pub pair2_stable_name: &'static str,
    pub volume_coin: f64,
    pub volume_stable: f64,
    pub volume_decimals_coin: u8,
    pub volume_decimals_stable: u8,
}
