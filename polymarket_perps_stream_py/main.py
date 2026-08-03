import asyncio
import aiohttp
import contextlib
import json
import time

import websockets
from websockets.exceptions import ConnectionClosed


# ------------ CONSTANTS ------------

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


# ----------- UTILITY FUNCTIONS -----------

def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}")

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
        self._prices: dict[str, float] = {}

    async def start(self):
        if not self._stream_task:
            log("Starting Polymarket Perps WebSocket stream...")
            self._stream_task = asyncio.create_task(self._stream())

    async def stop(self):
        if self._stream_task:
            log("Stopping Polymarket Perps WebSocket stream...")
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task
            self._stream_task = None

    async def subscribe_ticker(self, ticker: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._subscribers.setdefault(ticker, []).append(q)

        if ticker in self._prices:
            q.put_nowait(self._prices[ticker])

        return q
    
    async def unsubscribe_ticker(self, ticker: str, queue: asyncio.Queue) -> None:
        subs = self._subscribers.get(ticker)
        if subs:
            try:
                subs.remove(queue)
                if not subs:
                    del self._subscribers[ticker]
            except ValueError:
                log(f"{ticker} | Queue not found in subscribers list.")

    async def _heartbeat(self, ws):
        try:
            while True:
                await asyncio.sleep(30)
                await ws.send(json.dumps(PING))
        except Exception:
            pass

    async def _subscribe_all_tickers(self, ws):
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


        if abs(self._prices.get(symbol, 0) - index_price) < 1e-12:
            return

        self._prices[symbol] = index_price

        for queue in self._subscribers.get(symbol, []):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(index_price)


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

            except ConnectionClosed:
                log("Connection closed. Reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                log(f"Error in WebSocket stream: {e}. Reconnecting...")
                await asyncio.sleep(5)


# ---------- MAIN FUNCTION ----------

async def main():
    stream = PolymarketPerpsStream()
    await stream.start()

    symbols_to_subscribe = ['SP500-USD', 'GOLD-USD', 'WTIOIL-USD', 'NAS100-USD', 'SILVER-USD', 'BTC-USD', 'ETH-USD', 'SOL-USD', 'SPCX-USD', 'HYPE-USD', 'MU-USD', 'SKHY-USD', 'SKHYNIX-USD', 'AAPL-USD', 'MSFT-USD', 'GOOG-USD', 'AMZN-USD', 'NVDA-USD', 'META-USD', 'TSLA-USD', 'AMD-USD', 'INTC-USD', 'AVGO-USD', 'QCOM-USD', 'ARM-USD', 'TSM-USD', 'ASML-USD', 'SNDK-USD', 'DRAM-USD']
    symbol_queues = {}
    for symbol in symbols_to_subscribe:
        symbol_queues[symbol] = await stream.subscribe_ticker(symbol)

    tasks = {
        asyncio.create_task(q.get()): symbol
        for symbol, q in symbol_queues.items()
    }

    while True:
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            symbol = tasks.pop(task)
            price = task.result()

            log(f"{symbol:>11} | Price: {price}")

            tasks[asyncio.create_task(symbol_queues[symbol].get())] = symbol


if __name__ == "__main__":
    asyncio.run(main())