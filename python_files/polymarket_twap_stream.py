import websockets
import asyncio
import json
import time
from utils.logger import log

# ------------ CONSTANTS ------------

RECONNECT_DELAY = 5  # seconds

WS_URL = "wss://ws-live-data.polymarket.com"

# A time-weighted average price (TWAP) represents an asset’s price across a lookback window
SUPPORTED_TWAP_SYMBOLS = ['btc/usd', 'eth/usd', 'sol/usd', 'xrp/usd']


# ---------- POLYMARKET REAL-TIME DATA STREAM CLASS ----------

class PolymarketTimeWeightedAveragePriceStream:
    def __init__(self, ws_url=WS_URL):
        self.ws_url = ws_url
        self._stream_tasks: dict[str, asyncio.Task] = {}
        self._latest: dict[str, dict] = {}
        self._subscribers: dict[str, list[asyncio.Queue]] = {}

    async def start(self):
        for symbol in SUPPORTED_TWAP_SYMBOLS:
            if symbol not in self._stream_tasks:
                log(f"Starting stream for {symbol}...", level="INFO")
                self._stream_tasks[symbol] = asyncio.create_task(self._stream(symbol))
    
    async def stop(self):
        for symbol, task in self._stream_tasks.items():
            log(f"Stopping stream for {symbol}...", level="INFO")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                log(f"Stream for {symbol} stopped.", level="INFO")

    async def subscribe(self, ticker: str, window: int) -> asyncio.Queue:
        q : asyncio.Queue = asyncio.Queue(maxsize=1)
        key = self._key(ticker, window)
        self._subscribers.setdefault(key, []).append(q)

        if key in self._latest:
            q.put_nowait(self._latest[key])

        return q
    
    async def unsubscribe(self, ticker: str, window: int, queue: asyncio.Queue):
        key = self._key(ticker, window)
        subs = self._subscribers.get(key)
        if subs:
            try:
                subs.remove(queue)
                if not subs:
                    del self._subscribers[key]
            except ValueError:
                log(f"{key} | Queue not found in subscribers list.", level="WARNING")

    def _key(self, ticker: str, window: int) -> str:
        return f"{ticker}_{window}s"
        
    async def _subscribe_to_symbol(self, ws: websockets.ClientConnection, symbol: str):
        try:
            await ws.send(json.dumps({
                "action": "subscribe",
                "subscriptions": [
                        {
                            "topic": f"crypto_prices_twap_{window}",
                            "type": "update",
                            "filters": "{\"symbol\":\"" + symbol + "\"}",
                        }
                        for window in ("thirty", "sixty")
                    ],
            }))
            log(f"Subscribed to topics for {symbol}.")
        except Exception as e:
            log(f"Failed to subscribe to topics for {symbol}: {e}", level="ERROR")

    async def _handle_message(self, message: str):
        if not message:
            log("Received empty message.", level="WARNING")
            return
        try:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "update":
                payload = data['payload']
                symbol = payload['symbol']
                timestamp = payload['timestamp']
                price = payload['value']
                window = payload['window_s']

                key = self._key(symbol, window)

                if abs(self._latest.get(key, {}).get("price", 0) - price) < 1e-12:
                    return

                self._latest[key] = {
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "price": price,
                    "window": window
                }

                for queue in self._subscribers.get(key, []):
                    if queue.full():
                        try:
                            queue.get_nowait()  # Remove the old item
                        except asyncio.QueueEmpty:
                            pass
                    await queue.put({
                        "symbol": symbol,
                        "timestamp": timestamp,
                        "price": price,
                        "window": window
                    })

            else:
                log(f"Received non-update message: {data}", level="WARNING")
        except Exception as e:
            log(f"Error processing message: {e}. Message: {message}", level="ERROR")
       
    async def _stream(self, symbol: str):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    await self._subscribe_to_symbol(ws, symbol)
                    async for message in ws:
                        if isinstance(message, str):
                            await self._handle_message(message)
                        else:
                            log(f"Received non-string message: {message}", level="WARNING")

            except websockets.ConnectionClosed as e:
                log(f"Connection closed: {e}. Reconnecting in {RECONNECT_DELAY} seconds...", level="WARNING")
                await asyncio.sleep(RECONNECT_DELAY)
            except Exception as e:
                log(f"Unexpected error: {e}. Reconnecting in {RECONNECT_DELAY} seconds...", level="ERROR")
                await asyncio.sleep(RECONNECT_DELAY)


# ---------- MAIN FUNCTION ----------

async def main():

    async def consume(queue):
        last_price = None

        while True:
            data = await queue.get()
            price = data["price"]

            if last_price is not None and abs(last_price - price) / last_price < 0.0001:
                continue  # Skip if price change is less than 0.01%
            last_price = price

            datetime_str = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(data["timestamp"] / 1000),
            )

            key = f"{data['symbol']}_{data['window']}s"
            log(f"{key} | {datetime_str} | Price: {price}", level="INFO")

    stream = PolymarketTimeWeightedAveragePriceStream()
    await stream.start()

    consumers = []

    for symbol in SUPPORTED_TWAP_SYMBOLS:
        for window in (30, 60):
            queue = await stream.subscribe(symbol, window)
            consumers.append(asyncio.create_task(consume(queue)))

    await asyncio.gather(*consumers)


if __name__ == "__main__":
    asyncio.run(main())