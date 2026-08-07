use reqwest::Client;
// use serde_json::Value;
use serde::Deserialize;
use std::collections::HashMap;

// const RECONNECT_DELAY: u64 = 5; // seconds
// const WS_URL: &str = "wss://ws.perpetuals.polymarket.com/v1/ws";
const HTTP_URL: &str = "https://api.perpetuals.polymarket.com/v1";
// const SUBSCRIBE: &str = r#"{
//     "req": "sub",
//     "chs": ["tickers::all"]
// }"#;
// const PING: &str = r#"{
//     "req": "post",
//     "op": {
//         "type": "ping"
//     }
// }"#;


#[derive(Deserialize, Debug)]
struct TickerInfo {
    instrument_id: u32,
    symbol: String,
}


async fn get_iid_to_ticker_mapping() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let url = format!("{}/info/tickers", HTTP_URL);
    let client = Client::new();

    let response = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?;

    let tickers: Vec<TickerInfo> = response.json().await?;

    let mapping: HashMap<String, String> = tickers
        .into_iter()
        .map(|ticker| (ticker.instrument_id.to_string(), ticker.symbol))
        .collect();

    Ok(mapping)
}   


    

#[tokio::main]
async fn main() {
    let result = get_iid_to_ticker_mapping().await;
    println!("Résultat: {:?}", result);
}
