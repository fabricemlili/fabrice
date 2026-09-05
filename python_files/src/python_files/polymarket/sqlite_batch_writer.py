import asyncio
import sqlite3
import time
import queue
import threading
from collections.abc import Mapping
from pathlib import Path

from python_files.polymarket.perps_stream import PolymarketPerpsStream, SUPPORTED_SYMBOLS
from python_files.shared.logger import log


DB_PATH = Path(__file__).parent.parent / "data" / "market_data.db"
BATCH_SIZE = 500
BATCH_INTERVAL = 2.0 # seconds
ALERT_QUEUE_THRESHOLD = 1000
ALERT_DELAY_THRESHOLD = 5.0 # seconds


class SQLiteBatchWriter:
    def __init__(
        self,
        db_path: str | Path,
        columns: dict[str, str],
        index_columns: list[str] | None = None,
        batch_size: int = 200,
        batch_interval: float = 2.0,
        alert_queue_threshold: int = 1000,
        alert_delay_threshold: float = 5.0,
    ):
        if index_columns and not all(col in columns for col in index_columns):
            raise ValueError("All index_columns must be present in columns")
        self.index_columns = index_columns
        self.columns = columns
        self.db_path = Path(db_path)
        self.table_name = self.db_path.stem.replace("-", "_").replace(" ", "_")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.alert_queue_threshold = alert_queue_threshold
        self.alert_delay_threshold = alert_delay_threshold

        self._queue: "queue.Queue[dict]" = queue.Queue(maxsize=10_000)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.conn: sqlite3.Connection | None = None
        self._last_alert_time = 0.0

    @staticmethod
    def _init_db(db_path: Path, table_name: str, columns: dict, index_columns: list[str] | None) -> sqlite3.Connection:
        log(f"Initializing database {table_name} at {db_path}...", level="INFO")
        conn = sqlite3.connect(db_path)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS {} (
                {}
            )""".format(
            table_name,
            ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
        ))

        if index_columns:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_{}_{}
                ON {}({})
            """.format(
                table_name,
                "_".join(index_columns),
                table_name,
                ", ".join(index_columns)
            ))

        conn.commit()
        log(f"Database {table_name} initialized at {db_path}.", level="INFO")
        return conn

    def _save_batch(self, batch: list[dict]) -> None:
        log(f"Saving batch of {len(batch)} records to database...", level="INFO")
        if self.conn is None:
            raise RuntimeError("Database connection is not initialized")

        self.conn.executemany(
            """
            INSERT INTO {} (
                {}
            )
            VALUES ({})
            """.format(
                self.table_name,
                ", ".join(self.columns.keys()),
                ", ".join("?" for _ in self.columns)
            ),
            [
                tuple(data[col] for col in self.columns.keys())
                for data in batch
            ],
        )

        self.conn.commit()
        log(f"Batch of {len(batch)} records saved to database.", level="INFO")

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        if not self._thread.is_alive():
            log(f"Starting SQLiteBatchWriter thread for {self.table_name}...", level="INFO")
            self._thread.start()

    def stop(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            log(f"Stopping SQLiteBatchWriter thread for {self.table_name}...", level="INFO")
            self._stop_event.set()
            self._thread.join()

    def write(self, data: Mapping[str, object]) -> None:
        missing_columns = [col for col in self.columns if col not in data]
        if missing_columns:
            log(
                f"Invalid row for {self.table_name}: missing columns {missing_columns}. Dropping row.",
                level="WARNING",
            )
            return

        try:
            self._queue.put_nowait(dict(data))
            self._check_queue_health()
        except queue.Full:
            now = time.monotonic()
            if now - self._last_alert_time > 10.0:
                log(f"⚠️  Queue is full. Dropping data: {data}", level="WARNING")
                self._last_alert_time = now

    def _check_queue_health(self) -> None:
        queue_size = self._queue.qsize()
        if queue_size > self.alert_queue_threshold:
            now = time.monotonic()
            if now - self._last_alert_time > 10.0:
                log(f"⚠️  Queue size exceeded threshold: {queue_size} (> {self.alert_queue_threshold}) items pending", level="WARNING")
                self._last_alert_time = now

    def _run(self) -> None:
        self.conn = self._init_db(self.db_path, self.table_name, self.columns, self.index_columns)
        batch: list[dict] = []
        last_write = time.monotonic()

        try:
            while not self._stop_event.is_set() or not self._queue.empty():
                timeout = max(0.0, self.batch_interval - (time.monotonic() - last_write))
                try:
                    item = self._queue.get(timeout=timeout)
                    batch.append(item)
                except queue.Empty:
                    pass

                time_since_write = time.monotonic() - last_write
                if time_since_write > self.alert_delay_threshold and batch:
                    now = time.monotonic()
                    if now - self._last_alert_time > 10.0:
                        log(
                            f"⚠️  Queue delay exceeded threshold: {time_since_write:.2f}s (> {self.alert_delay_threshold}s) since last write, {len(batch)} items pending",
                            level="WARNING",
                        )
                        self._last_alert_time = now

                should_flush = batch and (
                    len(batch) >= self.batch_size
                    or time.monotonic() - last_write >= self.batch_interval
                )
                if should_flush:
                    self._save_batch(batch)
                    batch = []
                    last_write = time.monotonic()

            if batch:
                self._save_batch(batch)

        finally:
            if self.conn is not None:
                self.conn.close()
                self.conn = None


# ---------- MAIN FUNCTION ----------

async def run():

    async def consume(symbol: str, subscription_queue):
        try:
            while True:
                data = await subscription_queue.get()
                try:
                    db_writer.write(data)
                finally:
                    subscription_queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception:
            log(f"Consumer failed for {symbol}", level="ERROR")
            raise
            
    db_writer = SQLiteBatchWriter(
        db_path=DB_PATH,
        columns={
            "timestamp": "INTEGER NOT NULL",
            "symbol": "TEXT",
            "mark_price": "REAL NOT NULL",
            "index_price": "REAL NOT NULL",
        },
        index_columns=["timestamp", "symbol"],
        batch_size=BATCH_SIZE,
        batch_interval=BATCH_INTERVAL,
        alert_queue_threshold=ALERT_QUEUE_THRESHOLD,
        alert_delay_threshold=ALERT_DELAY_THRESHOLD,
    )
    db_writer.start()

    stream = PolymarketPerpsStream()
    consumers = []

    try:
        await stream.start()

        for symbol in SUPPORTED_SYMBOLS:
            subscription_queue = await stream.subscribe_ticker(symbol)
            consumers.append(asyncio.create_task(consume(symbol, subscription_queue)))

        await asyncio.gather(*consumers)

    except asyncio.CancelledError:
        log("Main task cancelled", level="INFO")

    finally:
        await stream.stop()

        for task in consumers:
            task.cancel()

        await asyncio.gather(*consumers, return_exceptions=True)

        db_writer.stop()
        log("Shutdown complete", level="INFO")


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n")


if __name__ == "__main__":
    main()