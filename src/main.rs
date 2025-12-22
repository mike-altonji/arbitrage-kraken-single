use dotenv::dotenv;
// use kraken_private::get_auth_token;
use std::env;
use telegram::send_telegram_message;

mod asset_pairs;
mod structs;
mod telegram;
mod utils;

#[tokio::main]
async fn main() {
    // Initialize setup
    dotenv().ok();
    let args: Vec<String> = env::args().collect();
    let allow_trades = args.contains(&"--trade".to_string());
    let use_colocated = args.contains(&"--colocated".to_string());

    // Determine WebSocket URLs based on --colocated flag
    let public_ws_url = if use_colocated {
        "wss://colo-london.vip-ws.kraken.com"
    } else {
        "wss://ws.kraken.com"
    };
    let private_ws_url = if use_colocated {
        "wss://colo-london.vip-ws-auth.kraken.com"
    } else {
        "wss://ws-auth.kraken.com"
    };

    utils::init_logging();
    let mode_message = if allow_trades {
        "💰 Launching Kraken arbitrage: Trade mode"
    } else {
        "🚀 Launching Kraken arbitrage: Evaluation-only mode"
    };
    send_telegram_message(mode_message).await;
    // let token = if allow_trades {
    //     Some(get_auth_token().await.expect("Could not pull auth token."))
    // } else {
    //     None
    // };

    // Initialize the listener threads
    let listener_handle0 = {
        let pairs0 = asset_pairs::ASSET_INDEX_0.keys();
        let pair_data_vec0 = Vec::new();

        tokio::spawn(async move {
            listener(pairs0, pair_data_vec0, public_ws_url)
                .await
                .expect("Listener failed");
        })
    };
}
