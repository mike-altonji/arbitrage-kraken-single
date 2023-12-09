use std::env;

pub async fn send_telegram_message(message: &str) {
    let bot_token = env::var("TELEGRAM_BOT_TOKEN").expect("TELEGRAM_BOT_TOKEN must be set");
    let chat_id = env::var("TELEGRAM_CHAT_ID").expect("TELEGRAM_CHAT_ID must be set");

    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}",
        bot_token, chat_id, message
    );

    if let Err(e) = reqwest::Client::new().post(&url).send().await {
        log::error!("Failed to send Telegram message: {:?}", e);
    }
}
