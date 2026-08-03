use time::OffsetDateTime;
use time::macros::format_description;
use std::collections::HashMap;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc::Sender, RwLock, watch};
use tokio::task::JoinHandle;
use tokio::sync::mpsc;


// ------------ CONSTANTS ------------

const WS_URL: &str = "wss://ws.perpetuals.polymarket.com/v1/ws";

const HTTP_URL: &str = "https://api.perpetuals.polymarket.com/v1";

const SUBSCRIBE: &str = r#"{
    "req": "sub",
    "chs": ["tickers::all"]
}"#;

const PING: &str = r#"{
    "req": "post",
    "op": {
        "type": "ping"
    }
}"#;


// ----------- UTILITY FUNCTIONS -----------

fn log(msg: &str) {
    let format = format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second]"
    );
    let now = OffsetDateTime::now_utc().format(&format).unwrap();
    println!("{} | {}", now, msg);
}

#[derive(Debug, Deserialize)]
struct TickerInfo {
    instrument_id: u64,
    symbol: String,
}

async fn get_iid_to_ticker_mapping(
) -> Result<HashMap<u64, String>, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/info/tickers", HTTP_URL);

    let client = Client::new();

    let resp: Vec<TickerInfo> = client.get(&url).send().await?.json().await?;

    let mut mapping = HashMap::new();

    for ticker in resp {
        mapping.insert(ticker.instrument_id, ticker.symbol);
    }

    Ok(mapping)
}


// ----------- POLYMARKET PERPS STREAM CLASS -----------

pub struct PolymarketPerpsStream {
    url: String,
    iid_to_ticker: RwLock<HashMap<u64, String>>,
    prices: RwLock<HashMap<String, f64>>,
    senders: RwLock<HashMap<String, watch::Sender<f64>>>,
    stream_task: Mutex<Option<JoinHandle<()>>>,
}


impl PolymarketPerpsStream {
    pub fn new(url: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            url: url.into(),
            iid_to_ticker: RwLock::new(HashMap::new()),
            prices: RwLock::new(HashMap::new()),
            senders: RwLock::new(HashMap::new()),
            stream_task: Mutex::new(None),
        })
    }
 
    pub fn with_default_url() -> Arc<Self> {
        Self::new(WS_URL)
    }
 
    pub async fn start(self: &Arc<Self>) {
        let mut guard = self.stream_task.lock().await;
        if guard.is_none() {
            log("Starting Polymarket Perps WebSocket stream...");
            let this = Arc::clone(self);
            *guard = Some(tokio::spawn(async move {
                this.run().await;
            }));
        }
    }

    pub async fn stop(self: &Arc<Self>) {
        let mut guard = self.stream_task.lock().await;
        if let Some(handle) = guard.take() {
            log("Stopping Polymarket Perps WebSocket stream...");
            handle.abort();
            let _ = handle.await;
        }
    }

    // async fn subscribe_all_tickers(
    //     &self,
    //     write: &mut WsWrite,
    //     read: &mut WsRead,
    // ) -> anyhow::Result<()> {
    //     write
    //         .send(Message::Text(subscribe_payload().to_string()))
    //         .await?;

    async fn run(&self) {
        if self.iid_to_ticker.read().await.is_empty() {
            match get_iid_to_ticker_mapping().await {
                Ok(mapping) => *self.iid_to_ticker.write().await = mapping,
                Err(e) => log(&format!("Error fetching iid->ticker mapping: {}", e)),
            }
        }

        // loop {
        //     let connected = connect_async(&self.url).await;
        //     let (ws_stream, _resp) = match connected {
        //         Ok(pair) => pair,
        //         Err(e) => {
        //             log(&format!("Error in WebSocket stream: {}. Reconnecting...", e));
        //             sleep(Duration::from_secs(5)).await;
        //             continue;
        //         }
        //     };

        //     let (mut write, mut read) = ws_stream.split();

            

        // }
    }
}




// ----------- MAIN FUNCTION -----------

fn main() {
    let mapping = tokio::runtime::Runtime::new().unwrap().block_on(get_iid_to_ticker_mapping());
    match mapping {
        Ok(map) => {
            for (iid, ticker) in map {
                log(&format!("Instrument ID: {}, Ticker: {}", iid, ticker));
            }
        }
        Err(e) => {
            log(&format!("Error: {}", e));
        }
    }
}
