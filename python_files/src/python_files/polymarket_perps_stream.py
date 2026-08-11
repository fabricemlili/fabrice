import asyncio
import aiohttp
import contextlib
import json
import time
import websockets
from python_files.logger import log


# ------------ PARAMETERS ------------
MIN_INDEX_PRICE_CHANGE_PERCENTAGE = 0.01
MIN_MARK_PRICE_CHANGE_PERCENTAGE = 0.01


# ------------ CONSTANTS ------------

RECONNECT_DELAY = 5  # seconds

WS_URL = "wss://ws.perpetuals.polymarket.com/v1/ws"

HTTP_URL = "https://api.perpetuals.polymarket.com/v1"

SUBSCRIBE = {
    "req": "sub",
    "chs": ["tickers::all"],
}

PING = {
    "req": "post",
    "op": {
        "type": "ping"
    },
}

SUPPORTED_SYMBOLS = [
    'SP500-USD', 'GOLD-USD', 'WTIOIL-USD', 'NAS100-USD', 'SILVER-USD', 
    'BTC-USD', 'ETH-USD', 'SOL-USD', 'SPCX-USD', 'HYPE-USD', 'MU-USD', 
    'SKHY-USD', 'SKHYNIX-USD', 'AAPL-USD', 'MSFT-USD', 'GOOG-USD', 'AMZN-USD', 
    'NVDA-USD', 'META-USD', 'TSLA-USD', 'AMD-USD', 'INTC-USD', 'AVGO-USD', 
    'QCOM-USD', 'ARM-USD', 'TSM-USD', 'ASML-USD', 'SNDK-USD', 'DRAM-USD'
]

# ----------- UTILITY FUNCTIONS -----------

async def get_iid_to_ticker_mapping() -> dict[str, str]:
    url = HTTP_URL + "/info/tickers"
    mapping: dict[str, str] = {}
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            resp = await response.json()

            for ticker_info in resp:
                iid = ticker_info.get("instrument_id")
                ticker = ticker_info.get("symbol")
                if iid and ticker:
                    mapping[iid] = ticker
                else:
                    raise ValueError(f"Invalid ticker info: {ticker_info}")
    return mapping


# ----------- POLYMARKET PERPS STREAM CLASS -----------

class PolymarketPerpsStream:
    def __init__(self, url=WS_URL):
        self.iid_to_ticker: dict[str, str] = {}
        self.url = url
        self._stream_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._subscribers: dict[str, list[asyncio.Queue]] = {}
        self._latest: dict[str, dict] = {}

    async def start(self):
        if not self._stream_task:
            log("Starting Polymarket Perps WebSocket stream...", level="INFO")
            self._stream_task = asyncio.create_task(self._stream())

    async def stop(self):
        if self._stream_task:
            log("Stopping Polymarket Perps WebSocket stream...", level="INFO")
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task
            self._stream_task = None

    async def subscribe_ticker(self, ticker: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._subscribers.setdefault(ticker, []).append(q)

        if ticker in self._latest:
            q.put_nowait(self._latest[ticker])

        return q
    
    async def unsubscribe_ticker(self, ticker: str, queue: asyncio.Queue) -> None:
        subs = self._subscribers.get(ticker)
        if subs:
            try:
                subs.remove(queue)
                if not subs:
                    del self._subscribers[ticker]
            except ValueError:
                log(f"{ticker} | Queue not found in subscribers list.", level="WARNING")

    async def _heartbeat(self, ws: websockets.ClientConnection):
        try:
            while True:
                await asyncio.sleep(30)
                await ws.send(json.dumps(PING))
        except Exception:
            pass

    async def _subscribe_all_tickers(self, ws: websockets.ClientConnection):
        await ws.send(json.dumps(SUBSCRIBE))
        sub_resp = json.loads(await ws.recv())
        sub_status = sub_resp.get("data", [{}])[0].get("status", [])
        if sub_status != "ok":
            raise Exception(f"Subscription failed: {sub_resp}")

    async def _handle_message(self, message):
        msg = json.loads(message)
        data = msg.get("data", {})

        if data.get("status") is not None:
            if data.get("status") != "ok":
                raise Exception(f"Connection error: {data}")
            return

        ts = msg.get("ts")
        iid = data.get("iid")
        index_price = data.get("idx")
        mark_price = data.get("mark")

        if any(e is None for e in [ts, iid, index_price, mark_price]):
            raise ValueError(f"Missing required fields in message: {msg}")
        else:
            symbol = self.iid_to_ticker.get(iid, "Unknown")
            index_price = float(index_price)
            mark_price = float(mark_price)

        if symbol in self._latest and abs(self._latest[symbol].get("index_price", 0) - index_price) < 1e-12:
            return

        self._latest[symbol] = {
            "symbol": symbol,
            "timestamp": ts,
            "index_price": index_price,
            "mark_price": mark_price
        }

        for queue in self._subscribers.get(symbol, []):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(self._latest[symbol])

    async def _stream(self):
        if not self.iid_to_ticker:
            self.iid_to_ticker = await get_iid_to_ticker_mapping()

        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    self._heartbeat_task = asyncio.create_task(self._heartbeat(ws))

                    try:
                        await self._subscribe_all_tickers(ws)
                        async for message in ws:
                            await self._handle_message(message)
                    finally:
                        if self._heartbeat_task:
                            self._heartbeat_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await self._heartbeat_task
                            self._heartbeat_task = None

            except websockets.ConnectionClosed as e:
                log(f"Connection closed: {e}. Reconnecting in {RECONNECT_DELAY} seconds...", level="WARNING")
                await asyncio.sleep(RECONNECT_DELAY)
            except Exception as e:
                log(f"Unexpected error: {e}. Reconnecting in {RECONNECT_DELAY} seconds...", level="ERROR")
                await asyncio.sleep(RECONNECT_DELAY)


# ---------- MAIN FUNCTION ----------

async def run():

    async def consume(queue):
        last_index_price = None
        last_mark_price = None

        while True:
            data = await queue.get()
            index_price = data["index_price"]
            mark_price = data["mark_price"]

            if (
                last_index_price is not None
                and last_mark_price is not None
                and abs(last_index_price - index_price) / last_index_price < MIN_INDEX_PRICE_CHANGE_PERCENTAGE / 100
                and abs(last_mark_price - mark_price) / last_mark_price < MIN_MARK_PRICE_CHANGE_PERCENTAGE / 100
            ):
                continue   # Skip logging if the price change is below the threshold

            last_index_price = index_price

            datetime_str = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(data["timestamp"] / 1000),
            )

            log(f"{data['symbol']:<11} | {datetime_str} | Index Price: {index_price:<8} | Mark Price: {mark_price:<8}", level="INFO")

    stream = PolymarketPerpsStream()
    await stream.start()

    consumers = []

    try:
        for symbol in SUPPORTED_SYMBOLS:
            queue = await stream.subscribe_ticker(symbol)
            consumers.append(asyncio.create_task(consume(queue)))

        await asyncio.gather(*consumers)

    except asyncio.CancelledError:
        log("Main task cancelled", level="INFO")

    finally:
        for task in consumers:
            task.cancel()
            
        await asyncio.gather(*consumers, return_exceptions=True)
        await stream.stop()
        log("Shutdown complete", level="INFO")


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n")


if __name__ == "__main__":
    main()