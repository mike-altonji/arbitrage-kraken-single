#[derive(Clone)]
pub struct PairData {
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub price_decimals: u8,
    pub volume_decimals: u8,
    pub order_min: f32,
    pub cost_min: f32,
    pub pair_status: bool,
}
pub type PairDataVec = Vec<PairData>;
