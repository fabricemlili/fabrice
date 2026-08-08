use std::collections::HashMap;
use serde::{Deserialize};
use reqwest::Client;
use anyhow::{Context, Result};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message::Text};

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio::task::JoinHandle;

use serde_json::Value;
use chrono::{Local, TimeZone};

mod logger;
use logger::logger::log;

// ============================================================
// CONSTANTS
// ============================================================

const WS_URL: &str = "wss://ws.perpetuals.polymarket.com/v1/ws";
const HTTP_URL: &str = "https://api.perpetuals.polymarket.com/v1";

const COMMAND_CHANNEL_CAPACITY: usize = 100;
const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 1;

const SUBSCRIBE: &str = r#"{
    "req": "sub",
    "chs": ["tickers::all"]
}"#;
// const PING: &str = r#"{
//     "req": "post",
//     "op": {
//         "type": "ping"
//     }
// }"#;

const SUPPORTED_SYMBOLS: &[&str] = &[
    "SP500-USD", "GOLD-USD", "WTIOIL-USD", "NAS100-USD", "SILVER-USD", 
    "BTC-USD", "ETH-USD", "SOL-USD", "SPCX-USD", "HYPE-USD", "MU-USD", 
    "SKHY-USD", "SKHYNIX-USD", "AAPL-USD", "MSFT-USD", "GOOG-USD", "AMZN-USD", 
    "NVDA-USD", "META-USD", "TSLA-USD", "AMD-USD", "INTC-USD", "AVGO-USD", 
    "QCOM-USD", "ARM-USD", "TSM-USD", "ASML-USD", "SNDK-USD", "DRAM-USD"
];

// ============================================================
// DATA STRUCTURES
// ============================================================

#[derive(Debug, Deserialize)]
struct TickerInfo {
    instrument_id: u64,
    symbol: String,
}

#[derive(Debug, Clone)]
struct TickerData {
    symbol: String,
    timestamp: u64,
    index_price: f64,
    mark_price: f64,
}

struct Subscription {
    sender: mpsc::Sender<TickerData>,
}

enum WsCommand {
    Subscribe {
        symbol: String,
        sender: mpsc::Sender<TickerData>,
    },

    Unsubscribe {
        symbol: String,
    },
}

// ============================================================
// HTTP: IID -> SYMBOL
// ============================================================

async fn get_iid_to_ticker_mapping() -> Result<HashMap<u64, String>> {
    let url = format!("{HTTP_URL}/info/tickers");
    let client = Client::new();
    let tickers: Vec<TickerInfo> = client
        .get(&url)
        .send()
        .await
        .context("failed to reach /info/tickers")?
        .json()
        .await
        .context("failed to parse /info/tickers response")?;
 
    Ok(tickers
        .into_iter()
        .map(|info| (info.instrument_id, info.symbol))
        .collect())
}

// ============================================================
// POLYMARKET PERPS STREAM
// ============================================================

struct PolymarketPerpsStream {
    stream_task: Option<JoinHandle<()>>,
    command_sender: Option<mpsc::Sender<WsCommand>>,
}

impl PolymarketPerpsStream {

    async fn new() -> Self {
        Self {
            stream_task: None,
            command_sender: None,
        }
    }

    async fn start(&mut self) -> Result<()> {

        if self.stream_task.is_none() {

            log("Starting Polymarket Perps WebSocket stream...", "info");

            let (command_sender, command_receiver) = mpsc::channel::<WsCommand>(COMMAND_CHANNEL_CAPACITY);

            let stream_task = tokio::spawn(async move {
                websocket_handler(command_receiver).await;
            });
            self.stream_task = Some(stream_task);
            self.command_sender = Some(command_sender);
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(task) = self.stream_task.take() {
            log("Stopping Polymarket Perps WebSocket stream...", "info");
            task.abort();
        }
        Ok(())
    }

    async fn subscribe(&self, symbol: &str) -> Result<mpsc::Receiver<TickerData>> {
        let (tx, rx) = mpsc::channel(SUBSCRIPTION_CHANNEL_CAPACITY);

        self.command_sender
            .as_ref()
            .expect("command sender not initialized")
            .send(WsCommand::Subscribe {
                symbol: symbol.to_string(),
                sender: tx,
            })
            .await
            .context("failed to send subscribe command")?;

        Ok(rx)
    }

    async fn unsubscribe(&self, symbol: &str) -> Result<()> {
        self.command_sender
            .as_ref()
            .expect("command sender not initialized")
            .send(WsCommand::Unsubscribe {
                symbol: symbol.to_string(),
            })
            .await
            .context("failed to send unsubscribe command")?;
        Ok(())
    }
} 

// ============================================================
// WEBSOCKET HANDLER
// ============================================================

async fn websocket_handler(mut command_receiver: mpsc::Receiver<WsCommand>) {

    let iid_to_ticker: HashMap<u64, String> = match get_iid_to_ticker_mapping().await {
        Ok(mapping) => mapping,
        Err(e) => {
            log(&format!("Error fetching IID to ticker mapping: {:?}", e), "error");
            return;
        }
    };

    let (mut ws_stream, _) = match connect_async(WS_URL).await {
        Ok(result) => result,

        Err(e) => {
            log(&format!("Failed to connect to WebSocket: {:?}", e), "error");
            return;
        }
    };

    if let Err(e) = ws_stream
        .send(Text(SUBSCRIBE.to_string().into()))
        .await
    {
        log(&format!("Failed to send subscribe message: {:?}", e), "error");
        return;
    }

    let mut subscriptions: HashMap<String, Subscription> = HashMap::new();
    let mut latest: HashMap<String, TickerData> = HashMap::new();

    loop {
        tokio::select! {

            // =================================================
            // HANDLE PolymarketPerpsStream COMMANDS
            // =================================================

            Some(command) = command_receiver.recv() => {
                match command {
                    WsCommand::Subscribe { symbol, sender } => {
                        subscriptions.insert(symbol.clone(), Subscription { sender });
                    }

                    WsCommand::Unsubscribe { symbol } => {
                        subscriptions.remove(&symbol);
                    }
                }
            }

            // =================================================
            // HANDLE WEBSOCKET MESSAGES
            // =================================================

            Some(msg) = ws_stream.next() => {
                match msg {
                    Ok(msg) => {
                        handle_ws_message(msg, &iid_to_ticker, &mut subscriptions, &mut latest).await;
                    }
                    Err(e) => {
                        log(&format!("WebSocket error: {:?}", e), "error");
                        break;
                    }
                }
            }
        }
    }
}

// ============================================================
// HANDLE WEBSOCKET MESSAGES
// ============================================================

async fn handle_ws_message(
    msg: tokio_tungstenite::tungstenite::Message,
    iid_to_ticker: &HashMap<u64, String>,
    subscriptions: &mut HashMap<String, Subscription>,
    latest: &mut HashMap<String, TickerData>,
) {
    if !msg.is_text() {
        return;
    }

    let text = match msg.into_text() {
        Ok(text) => text,
        Err(err) => {
            log(&format!("Failed to read message text: {err}"), "error");
            return;
        }
    };

    let json: Value = match serde_json::from_str(&text) {
        Ok(json) => json,
        Err(err) => {
            log(&format!("Invalid JSON: {err}"), "error");
            return;
        }
    };

    if let Some(data) = json.get("data") {
        if let Some(iid) = data.get("iid").and_then(|v| v.as_u64()) {
            let symbol = iid_to_ticker.get(&iid).cloned().unwrap_or_else(|| "Unknown".to_string());

            let mark_price: f64 = data.get("mark").and_then(|v| v.as_str()).and_then(
                |s| s.parse::<f64>().ok()
            ).unwrap();
            let index_price: f64 = data.get("idx").and_then(|v| v.as_str()).and_then(
                |s| s.parse::<f64>().ok()
            ).unwrap();
            let timestamp: u64 = json.get("ts").and_then(|v| v.as_u64()).unwrap();

            if let Some(latest_ticker) = latest.get(&symbol) {
                if (latest_ticker.index_price - index_price).abs() < 1e-12 {
                    return;
                }
            }
            
            if let Some(subscription) = subscriptions.get(&symbol) {
                let ticker_data = TickerData {
                    symbol: symbol.clone(),
                    timestamp,
                    index_price,
                    mark_price,
                };
                latest.insert(symbol.clone(), ticker_data.clone());
                if let Err(e) = subscription.sender.send(ticker_data).await {
                    log(&format!("Failed to send ticker data for {}: {:?}", symbol, e), "error");
                }
            }
        } else if let Some(array) = data.as_array() {
            if array.len() == 1 && array[0].get("status").and_then(|v| v.as_str()) != Some("ok") {
                log(&format!("Connection error: {:?}", array[0]), "error");
            }
        } else {
            log(&format!("Unexpected data format: {:?}", data), "error");
        }
    }
}

// ============================================================
// MAIN FUNCTION
// ============================================================

#[tokio::main]
async fn main() {

    async fn consume(mut rx: mpsc::Receiver<TickerData>) {
        while let Some(ticker_data) = rx.recv().await {
            log(
                &format!(
                    "{:<11} | {} | Index Price: {:<8} | Mark Price: {:<8}",
                    ticker_data.symbol,
                    Local
                        .timestamp_opt(ticker_data.timestamp as i64 / 1000, 0)
                        .single()
                        .expect("invalid timestamp")
                        .format("%Y-%m-%d %H:%M:%S"),
                    ticker_data.index_price,
                    ticker_data.mark_price
                ),
                "info"
            );
        }
    }

    let mut stream = PolymarketPerpsStream::new().await;
    let mut handles = Vec::new();

    if let Err(e) = stream.start().await {
        log(&format!("Error starting stream: {:?}", e), "error");
        return;
    }

    for symbol in SUPPORTED_SYMBOLS {
        match stream.subscribe(symbol).await {
            Ok(rx) => {
                handles.push(tokio::spawn(consume(rx)));
            }
            Err(e) => {
                log(
                    &format!("Error subscribing to {}: {:?}", symbol, e),
                    "error",
                );
            }
        }
    }

    sleep(Duration::from_secs(60)).await;

    for symbol in SUPPORTED_SYMBOLS {
        if let Err(e) = stream.unsubscribe(symbol).await {
            log(
                &format!("Error unsubscribing from {}: {:?}", symbol, e),
                "error",
            );
        }
    }

    if let Err(e) = stream.stop().await {
        log(&format!("Error stopping stream: {:?}", e), "error");
        return;
    }

    for handle in handles {
        if let Err(e) = handle.await {
            log(&format!("Consumer task failed: {:?}", e), "error");
        }
    }
}