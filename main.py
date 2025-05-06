#!/usr/bin/env python3
import hashlib
import json
import logging
import multiprocessing as mp
import os
import signal
import shutil
import sys
import tempfile
import time

from datetime import date, datetime, timezone, timedelta
from multiprocessing import Process, Queue
from pathlib import Path

import pandas as pd
import yfinance as yf
from pydantic import ValidationError
from shared_schemas import TradeInputModel
from watchdog.events import (
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer
from yfinance.exceptions import YFRateLimitError
from zoneinfo import ZoneInfo

# ─── Configuration ─────────────────────────────────────────────────────────────

ALLOWED_EXTS        = {".json"}
CACHE_FILENAME      = ".dir_cache.json"

TRADE_INPUT_DIR     = Path(os.getenv("TRADE_INPUT_DIR", "trade_speculations"))
TRADES_CSV          = Path(os.getenv("TRADES_CSV", "trades.csv"))
TIME_SERIES_DIR     = Path(os.getenv("TIME_SERIES_DIR", "time_series"))
LEDGER_PATH         = Path(os.getenv("LEDGER_PATH", "trade_ledger.json"))
TRASHED_PATH        = Path(os.getenv("TRASHED_PATH", "trashed_trades.json"))

DEBOUNCE_SECONDS    = float(os.getenv("DEBOUNCE_SECONDS", ".5"))
SCHEDULE_SLOTS      = ["06:30", "09:30", "12:30"]           # 3x per trading day

TIMEZONE            = ZoneInfo("America/Los_Angeles")

AUTO_ARCHIVED_DIR   = TRADE_INPUT_DIR / "auto_archived_trades"
AUTO_ARCHIVED_DIR.mkdir(exist_ok=True, parents=True)

# ─── Utilities ─────────────────────────────────────────────────────────────────

def current_timestamp() -> str:
    """
    Formatting for prints
    """
    # new — ISO8601 to the second:
    return datetime.now(TIMEZONE) \
                 .isoformat(timespec='seconds') \
                 .replace("+00:00","Z")
    
    # human-friendly with UTC label. NOT ISO 8601 STANDARD
    # return datetime.now(TIMEZONE) \
    #              .strftime("%Y-%m-%d %H:%M:%S UTC")

def log_event(event: dict):
    """
    Stamp and emit a uniform JSON log:
      - worker   : string
      - action   : string
      - trade_id : optional
      + timestamp (added here)
    """
    event.setdefault("timestamp", current_timestamp())
    print(json.dumps(event), flush=True)


# ─── Directory Monitor Process ─────────────────────────────────────────────────

class DirectoryMonitorProcess(mp.Process):
    """
    Watches TRADE_INPUT_DIR for JSON creations/modifications/deletions,
    validates & debounces them, then pushes clean payloads to ledger_queue.
    """
    def __init__(self, input_dir: Path, ledger_queue: mp.Queue):
        super().__init__(name="dir_watchdog_process", daemon=True)
        self.input_dir    = Path(input_dir)
        self.ledger_queue = ledger_queue
        self._cache       = {}   # trade_id → {hash, path}
        self._path_map    = {}   # path → trade_id
        self._last_seen   = {}   # path → timestamp
        self.cache_path   = self.input_dir / CACHE_FILENAME

    def compute_hash(self, obj: dict) -> str:
        """
        Serialize obj with sorted keys and convert date/datetime → ISO strings,
        then SHA-256 hash the result.
        """
        def _converter(o):
            if isinstance(o, (datetime, date)):
                return o.isoformat()
            raise TypeError(f"Type {type(o)} not serializable")

        serialized = json.dumps(obj, sort_keys=True, default=_converter).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()

    def normalize_trade_id(self, tid: str) -> str:
        return tid.strip().upper()

    def process_file(self, path: str):
        # 1. Filter out ADS (Zone.Identifier streams)
        if ":" in path:
            return

        # 2. Skip until file actually has non-whitespace content
        try:
            with open(path, encoding="utf-8") as f:
                text = f.read()
        except OSError:
            return

        if not text.strip():
            return

        # 3. Only JSON
        ext = Path(path).suffix.lower()
        if ext not in ALLOWED_EXTS:
            return

        # 4. Try to parse JSON
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            # partial write → wait for next modify
            return

        # 5. Normalize trade_id
        tid = self.normalize_trade_id(raw.get("trade_id", ""))
        raw["trade_id"] = tid

        # 6. Validate schema
        try:
            validated = TradeInputModel(**raw)
        except ValidationError as ve:
            msgs = []
            for err in ve.errors():
                loc = ".".join(str(x) for x in err["loc"])
                msgs.append(f"{loc}: {err['msg']}")
            log_event({
                "worker":    self.name,
                "action":    "validation_failed",
                "trade_id":  tid or "UNKNOWN",
                "error":     "; ".join(msgs),
                "timestamp": current_timestamp()
            })
            return

        # 7. Skip if no change
        clean = validated.model_dump(mode="json")
        canonical = json.dumps(clean, sort_keys=True)
        new_hash = self.compute_hash(canonical)

        old = self._cache.get(tid)
        if old and old["hash"] == new_hash:
            log_event({
                "worker":   self.name,
                "action":   "skipped_no_change",
                "trade_id": tid
            })
            return

        # 8. Update cache & queue for ledger
        self._cache[tid]     = {"hash": new_hash, "path": path}
        self._path_map[path] = tid

        # serialize via model_dump_json, then parse back into a pure-Python dict
        trade_json = validated.model_dump_json()
        payload = {
            "source": self.name,
            "trade_id": tid,
            "validated_trade": json.loads(trade_json),
            "detected_at": current_timestamp()
        }
        self.ledger_queue.put(payload)

        # 9. Log that we buffered it
        log_event({
            "worker":   self.name,
            "action":   "buffered_sent_to_ledger",
            "trade_id": tid
        })

    def process_deletion(self, path: str):
        tid = self._path_map.pop(path, None)
        if not tid:
            return
        self._cache.pop(tid, None)
        # notify ledger of deletion
        self.ledger_queue.put({
            "source": self.name,
            "trade_id": tid,
            "action": "deleted"
        })
        log_event({"worker": self.name, "action": "file_deleted", "trade_id": tid})

    def _load_persistent_cache(self):
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                saved = json.load(f)
            # rebuild in-memory maps
            self._cache = saved
            self._path_map = {info["path"]: tid for tid, info in saved.items()}
        except FileNotFoundError:
            # no cache yet → start fresh
            return
        except Exception as e:
            log_event({
                "worker": self.name,
                "action": "cache_load_failed",
                "error": str(e),
                "trade_id": None
            })

    def _save_persistent_cache(self):
        try:
            tmp = self.cache_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._cache, f)
            os.replace(tmp, self.cache_path)
        except Exception as e:
            log_event({
                "worker": self.name,
                "action": "cache_save_failed",
                "error": str(e),
                "trade_id": None
            })

    def initial_scan(self):
        """
        On startup, walk every JSON file in input_dir,
        compare hashes to cached values, and enqueue any new/changed.
        """
        for p in self.input_dir.iterdir():
            path_str = str(p)
            if p.suffix.lower() not in ALLOWED_EXTS:
                continue
            # skip ADS or zero-byte
            if ":" in path_str:
                continue
            try:
                if p.stat().st_size == 0:
                    continue
            except OSError:
                continue

            # load + normalize + validate (same as process_file)
            try:
                raw = json.load(open(p, encoding="utf-8"))
            except Exception:
                continue

            tid = self.normalize_trade_id(raw.get("trade_id", ""))
            raw["trade_id"] = tid

            try:
                validated = TradeInputModel(**raw)
            except ValidationError as ve:
                # Build a concise human‐readable summary
                error_msgs = []
                for err in ve.errors():
                    # err["loc"] is a tuple like ("rationale","iv_rank")
                    loc = ".".join(str(x) for x in err["loc"])
                    error_msgs.append(f"{loc}: {err['msg']}")

                log_event({
                    "worker":    self.name,
                    "action":    "validation_failed",
                    "trade_id":  tid or "UNKNOWN",
                    "error":     "; ".join(error_msgs),
                    # you can also include the full error objects if you like:
                    "details":   ve.errors()
                })
                continue

            clean = validated.model_dump(mode="json")
            canonical = json.dumps(clean, sort_keys=True)
            new_hash = self.compute_hash(canonical)

            old = self._cache.get(tid)
            if not old or old["hash"] != new_hash:
                # update cache & path_map
                self._cache[tid]    = {"hash": new_hash, "path": path_str}
                self._path_map[path_str] = tid

                # enqueue
                # serialize via model_dump_json, then parse back into a pure-Python dict
                trade_json = validated.model_dump_json()
                payload = {
                    "source": self.name,
                    "trade_id": tid,
                    "validated_trade": json.loads(trade_json),
                    "detected_at": current_timestamp()
                }
                self.ledger_queue.put(payload)
                log_event({
                    "worker": self.name,
                    "action": "buffered_sent_to_ledger_startup",
                    "trade_id": tid
                })

        # persist whatever we just merged
        self._save_persistent_cache()

    def run(self):
        # ensure folder and load last-run cache
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self._load_persistent_cache()

        # catch up on anything new/changed while we were down
        self.initial_scan()

        # now start watching for live events
        handler = FileSystemEventHandler()
        handler.on_created  = lambda e: self.process_file(e.src_path)
        handler.on_modified = lambda e: self.process_file(e.src_path)
        handler.on_deleted  = lambda e: self.process_deletion(e.src_path)

        obs = Observer()
        obs.schedule(handler, str(self.input_dir), recursive=False)
        obs.start()
        log_event({"worker": self.name, "action": "running", "trade_id": None})

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            obs.stop()
            obs.join()
            log_event({"worker": self.name, "action": "exited", "trade_id": None})

# ─── Worker that fetches & writes CSVs ──────────────────────────────────────────
class YFinanceWorker:
    """
    Fetch market data for active trades and append to per-ticker CSV.

    Parameters:
    - trade_ledger_path: Path to JSON file containing trade records
    - output_dir: Directory to write per-ticker CSVs
    - rsi_period: Look-back period for RSI calculation
    - date_field: Optional key name in JSON for the expiration date
    """
    def __init__(
        self,
        trade_ledger_path: Path,
        output_dir: Path,
        rsi_period: int = 14,
        date_field: str = None
    ):
        self.trade_ledger_path = trade_ledger_path
        self.output_dir = output_dir
        self.rsi_period = rsi_period
        self.date_field = date_field  # explicit JSON key for date
        self.name = "fetch_ticker_process"
        self.option_chain_cache = {}  # (ticker, expiry_date) → OptionChain


        self.output_dir.mkdir(parents=True, exist_ok=True)
        logging.getLogger().setLevel(logging.INFO)

    def _append_option_chain(self, trade: dict, data: dict, chain):
        """
        Write out one row per contract (call or put) to
        {ticker}_options.csv, keeping trade_id, snapshot, ticker,
        price, sma, rsi, plus all option fields.
        """
        ticker    = trade["ticker"]
        ts        = data["snapshot_timestamp"]
        price     = data["price"]
        sma       = data["sma"]
        rsi       = data["rsi"]
        out_file  = self.output_dir / f"{ticker}_options.csv"
        tmp_file  = out_file.with_suffix(".csv.tmp")

        # build a single DataFrame of all options
        calls = chain.calls.copy()
        puts  = chain.puts.copy()
        calls["contract_type"] = "call"
        puts["contract_type"]  = "put"
        opts = pd.concat([calls, puts], ignore_index=True)

        # inject your trade‐level columns
        opts["trade_id"]          = trade["trade_id"]
        opts["snapshot_timestamp"] = ts
        opts["ticker"]            = ticker
        opts["price"]             = price
        opts["sma"]               = sma
        opts["rsi"]               = rsi

        # reorder so these come first
        front = ["trade_id", "snapshot_timestamp", "ticker", "price", "sma", "rsi", "contract_type"]
        rest  = [c for c in opts.columns if c not in front]
        opts   = opts[ front + rest ]

        # idempotent upsert: one row per (trade_id, snapshot, contractSymbol)
        if out_file.exists():
            old = pd.read_csv(out_file, parse_dates=["snapshot_timestamp"])
            mask = ~(
                (old["trade_id"] == trade["trade_id"]) &
                (old["snapshot_timestamp"] == ts) &
                (old["contractSymbol"].isin(opts["contractSymbol"]))
            )
            df = pd.concat([old[mask], opts], ignore_index=True)
        else:
            df = opts

        # atomic write
        with open(tmp_file, "w", newline="") as f:
            df.to_csv(f, index=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp_file, out_file)
        log_event({
            "worker":   self.name,
            "action":   "options_upserted",
            "trade_id": trade["trade_id"],
            "ticker":   ticker,
            "timestamp": ts
        })

    def _append_underlying(self, trade: dict, data: dict):
        """
        Append summary snapshot (price, sma, rsi) for a trade to the per-ticker CSV.
        Does NOT include option chain details.
        """
        ticker   = trade["ticker"]
        out_file = self.output_dir / f"{ticker}_underlying.csv"
        tmp_file = out_file.with_suffix(".csv.tmp")

        # Build the summary row
        row = {
            "trade_id": trade["trade_id"],
            "snapshot_timestamp": data["snapshot_timestamp"],
            "price": data["price"],
            "sma": data["sma"],
            "rsi": data["rsi"]
        }
        new_df = pd.DataFrame([row])

        # Load existing summaries and upsert summary row
        if out_file.exists():
            old = pd.read_csv(out_file, parse_dates=["snapshot_timestamp"])
            mask = ~(
                (old["trade_id"] == row["trade_id"]) &
                (old["snapshot_timestamp"] == row["snapshot_timestamp"])
            )
            df = pd.concat([old[mask], new_df], ignore_index=True)
        else:
            df = new_df

        # Atomic write
        with open(tmp_file, "w", newline="") as f:
            df.to_csv(f, index=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp_file, out_file)
        logging.info(f"[{self.name}] Upserted summary for {ticker} @ {row['snapshot_timestamp']}")

    def process_trades(self, trades: list[dict]):
        """
        Bulk‐download history in chunks and cache option chains to avoid rate limits.
        3 chunks of 10 tickers
        each chunk: ~10 history requests (serialized) + option chain calls
        60-second pause between chunks
        """
        # Configuration: how many tickers per download, and how long to wait between them
        max_chunk = int(os.getenv("MAX_TICKERS_PER_CHUNK", "10"))
        chunk_delay = float(os.getenv("CHUNK_DELAY_SECONDS", "60"))

        # Get unique tickers and split into chunks
        tickers = sorted({t["ticker"] for t in trades})
        if not tickers:
            return

        # Build a map from ticker -> its trades
        trades_by_ticker = {}
        for t in trades:
            trades_by_ticker.setdefault(t["ticker"], []).append(t)
            

        # Process each chunk sequentially
        for i in range(0, len(tickers), max_chunk):
            chunk = tickers[i:i + max_chunk]
            try:
                # threads=False forces serial requests under the hood
                bulk = yf.download(
                    tickers=chunk,
                    period="5d",
                    interval="1m",
                    group_by="ticker",
                    threads=False
                )
            except Exception as e:
                log_event({
                    "worker": self.name,
                    "action": "bulk_history_failed",
                    "tickers": chunk,
                    "error": str(e)
                })
                # wait before next chunk
                time.sleep(chunk_delay)
                continue

            for ticker in chunk:
                # extract the right sub-DataFrame
                try:
                    df = bulk if len(chunk) == 1 else bulk[ticker]
                except KeyError:
                    log_event({
                        "worker": self.name,
                        "action": "history_missing",
                        "ticker": ticker
                    })
                    continue

                # if no data, skip
                if "Close" not in df or df["Close"].empty:
                    log_event({
                        "worker": self.name,
                        "action": "no_close_data",
                        "ticker": ticker
                    })
                    continue

                close = df["Close"]
                min_len = max(self.rsi_period, 50)
                if len(close) < min_len:
                    log_event({
                        "worker": self.name,
                        "action": "insufficient_history",
                        "ticker": ticker,
                        "rows": len(close)
                    })
                    continue

                price = float(close.iloc[-1])
                sma   = float(close.rolling(50).mean().iloc[-1])
                rsi   = self.calculate_rsi(close, self.rsi_period)

                # cached option chain
                # expiry = trades_by_ticker[ticker][0]["expiration_date"]
                # cache_key = (ticker, expiry)
                # chain = self.option_chain_cache.get(cache_key)

                raw_exp = t["expiration_date"]
                if isinstance(raw_exp, (datetime, date)):
                    expiry = raw_exp.strftime("%Y-%m-%d")
                else:
                    expiry = str(raw_exp)

                cache_key = (ticker, expiry)
                chain = self.option_chain_cache.get(cache_key)
                if chain is None:
                    try:
                        chain = yf.Ticker(ticker).option_chain(expiry)
                        self.option_chain_cache[cache_key] = chain
                    except YFRateLimitError:
                        log_event({
                            "worker": self.name,
                            "action": "rate_limit_chain",
                            "ticker": ticker
                        })
                        continue

                data = {
                    "snapshot_timestamp": current_timestamp(),
                    "price":              price,
                    "sma":                sma,
                    "rsi":                rsi,
                    "calls":              chain.calls,
                    "puts":               chain.puts
                }

                # upsert every trade for this ticker
                for t in trades_by_ticker[ticker]:
                    self._append_underlying(t, data)
                    self._append_option_chain(t, data, chain)
                    log_event({
                        "worker":   self.name,
                        "action":   "underlying_upserted",
                        "trade_id": t["trade_id"],
                        "ticker":   ticker
                    })

            # throttle before next chunk
            time.sleep(chunk_delay)

    def calculate_rsi(self, series: pd.Series, period: int | None = None) -> float:
        """
        original RSI formula introduced by J. Welles Wilder employs a smoothed moving average (SMMA), 
        which is akin to an exponential moving average (EMA) with a smoothing factor α = 1 / n, where n is the period length. 
        This method gives more weight to recent price changes and is considered more accurate in reflecting market momentum
        """
        # allow caller to omit period
        if period is None:
            period = self.rsi_period
        delta = series.diff().dropna()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ema_up = up.ewm(alpha=1/period, min_periods=period).mean()
        ema_down = down.ewm(alpha=1/period, min_periods=period).mean()
        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

    def load_trades(self) -> list[dict]:
        """
        Read the JSON ledger from self.trade_ledger_path (dict or list),
        validate via Pydantic, and return all valid trades.
        """
        # 1) Load raw JSON
        try:
            text = self.trade_ledger_path.read_text(encoding="utf-8")
            raw  = json.loads(text)
        except Exception as e:
            log_event({
                "worker": self.name,
                "action": "load_ledger_failed",
                "error": str(e)
            })
            return []

        # 2) Normalize to a list of records
        if isinstance(raw, dict):
            records = list(raw.values())
        elif isinstance(raw, list):
            records = raw
        else:
            log_event({
                "worker": self.name,
                "action": "ledger_unexpected_format",
                "error": f"Expected dict or list, got {type(raw)}"
            })
            return []

        # 3) Validate each via TradeInputModel
        valid: list[dict] = []
        for entry in records:
            try:
                trade = TradeInputModel(**entry).model_dump()
                valid.append(trade)
            except ValidationError as ve:
                errs = "; ".join(f"{e['loc'][0]}: {e['msg']}" for e in ve.errors())
                log_event({
                    "worker":   self.name,
                    "action":   "trade_validation_failed",
                    "trade_id": entry.get("trade_id"),
                    "error":    errs
                })

        # 4) Return every validated trade unfiltered
        return valid
    
    def is_expired(self, expiration_date) -> bool:
        """
        Determine if a trade is expired based on its expiration date.
        Accepts ISO strings, date, or datetime objects.
        If that next_fetch date is *after* your expiry date, you’re too late. signal auto close.
        """
        # normalize to date
        if isinstance(expiration_date, str):
            exp = datetime.fromisoformat(expiration_date).date()
        elif isinstance(expiration_date, datetime):
            exp = expiration_date.date()
        else:
            exp = expiration_date  # assume it's a date
        return datetime.now(TIMEZONE).date() > exp

    def should_auto_close(self, expiration_date) -> bool:
        """
        Determine if a trade should auto-close (next fetch would be past expiry).
        Accepts ISO strings, date, or datetime objects.
        """
        if isinstance(expiration_date, str):
            exp = datetime.fromisoformat(expiration_date)
        elif isinstance(expiration_date, datetime):
            exp = expiration_date
        else:
            # assume date
            exp = datetime.combine(expiration_date, datetime.min.time(), tzinfo=TIMEZONE)
        next_fetch = datetime.now(TIMEZONE) + timedelta(hours=3)
        return next_fetch.date() > exp.date()

# ─── FetchTickerProcess ────────────────────────────────────────────────────────
class FetchTickerProcess(mp.Process):
    def __init__(self,
                 debug_interval: int = None
                ):
        super().__init__(name="fetch_ticker_process", daemon=True)
        self.worker    = YFinanceWorker(trade_ledger_path=LEDGER_PATH, output_dir=TIME_SERIES_DIR)
        self._last_run = {slot: None for slot in SCHEDULE_SLOTS}
        self.debug_interval = debug_interval

    def run(self):
        if self.debug_interval is not None:
            self._run_interval_mode()
        else:
            self._run_schedule_mode()
    
    def _run_interval_mode(self):
        log_event({
            "worker":   self.name,
            "action":   "debug_interval_started",
            "interval": self.debug_interval
        })
        try:
            while True:
                # pull all trades and bulk‐fetch
                trades = self.worker.load_trades()
                self.worker.process_trades(trades)

                # if you still want your expiry/auto‐close logging here:
                for t in trades:
                    if self.worker.is_expired(t["expiration_date"]):
                        self._archive_trade(t, "Expired")
                    elif self.worker.should_auto_close(t["expiration_date"]):
                        self._archive_trade(t, "Auto Closed")

                time.sleep(self.debug_interval)
        except KeyboardInterrupt:
            pass
        finally:
            log_event({"worker": self.name, "action": "debug_interval_exited"})
    
    def _run_schedule_mode(self):
        """
        At each scheduled UTC slot, load *all* active trades and process them in one batch.
        """
        log_event({"worker": self.name, "action": "started", "trade_id": None})
        try:
            while True:
                now_utc = datetime.now(TIMEZONE)
                hhmm    = now_utc.strftime("%H:%M")
                today   = now_utc.date()

                # Only trigger once per slot per day
                if hhmm in SCHEDULE_SLOTS and self._last_run.get(hhmm) != today:
                    trades = self.worker.load_trades()               # ← all trades, not a slice
                    log_event({
                        "worker": self.name,
                        "action": "scheduled_run",
                        "slot":   hhmm,
                        "count":  len(trades)
                    })

                    # Bulk-fetch every ticker in one shot
                    self.worker.process_trades(trades)

                    # Then still do your expiry / auto-close logging
                    for t in trades:
                        exp = t["expiration_date"]
                        if self.worker.is_expired(exp):
                            self._archive_trade(t, "Expired")
                        elif self.worker.should_auto_close(exp):
                            self._archive_trade(t, "Auto Closed")

                    self._last_run[hhmm] = today

                time.sleep(60)
        except KeyboardInterrupt:
            pass
        finally:
            log_event({"worker": self.name, "action": "exited", "trade_id": None})

    def _archive_trade(self, trade: dict, new_status: str):
        """
        Update the trade JSON’s status and move it into past_trades/.
        """
        src = TRADE_INPUT_DIR / f"{trade['trade_id']}.json"
        dst = AUTO_ARCHIVED_DIR / src.name
        try:
            # 1) Update status field
            data = json.loads(src.read_text(encoding="utf-8"))
            data['status'] = new_status
            src.write_text(json.dumps(data, indent=2), encoding="utf-8")

            # 2) Move file
            shutil.move(str(src), str(dst))

            log_event({
                "worker":   self.name,
                "action":   f"archived_{new_status.lower()}",
                "trade_id": trade["trade_id"],
                "filename": dst.name
            })
        except Exception as e:
            log_event({
                "worker":   self.name,
                "action":   "archive_failed",
                "trade_id": trade["trade_id"],
                "error":    str(e)
            })

# ─── Ledger Updater Process ────────────────────────────────────────────────────

class LedgerUpdaterProcess(Process):
    """
    Listens on `ledger_queue` for messages from dir_watchdog_process:

    1) Upsert message (new or changed trade):
       {
         "source":            "dir_watchdog_process",
         "trade_id":          "<TRADE_ID>",
         "validated_trade":   { ... },
         "detected_at":       "2025-05-02T14:00:00Z"
       }

    2) Deletion message:
       {
         "source":    "dir_watchdog_process",
         "trade_id":  "<TRADE_ID>",
         "action":    "deleted"
       }
    """
    def __init__(self,
                 ledger_queue: Queue,
                 ledger_path: str,
                 trashed_path: str,
                 read_retries: int = 3,
                 retry_delay: float = 0.1):
        super().__init__(name="ledger_updater_process", daemon=True)
        self.ledger_queue = ledger_queue
        self.ledger_path  = ledger_path
        self.trashed_path = trashed_path
        self.read_retries = read_retries
        self.retry_delay  = retry_delay

        # bootstrap both ledger and trashed files if missing
        if not os.path.exists(self.ledger_path):
            self._atomic_write_file(self.ledger_path, {})
            log_event({
                "worker": self.name,
                "action": "ledger_bootstrapped",
                "path": self.ledger_path
            })
        if not os.path.exists(self.trashed_path):
            self._atomic_write_file(self.trashed_path, {})
            log_event({
                "worker": self.name,
                "action": "trashed_bootstrapped",
                "path": self.trashed_path
            })

    def run(self):
        log_event({
            "worker":    self.name,
            "action":    "running",
            "timestamp": datetime.now(TIMEZONE).isoformat().replace("+00:00","Z")
        })

        while True:
            try:
                msg = self.ledger_queue.get()    # <— may raise KeyboardInterrupt on Ctrl+C
            except KeyboardInterrupt:
                log_event({
                    "worker":    self.name,
                    "action":    "shutdown_triggered",
                    "timestamp":  datetime.now(TIMEZONE).isoformat().replace("+00:00","Z")
                })
                break
            if msg is None:
                log_event({
                    "worker":    self.name,
                    "action":    "shutdown_received",
                    "timestamp": datetime.now(TIMEZONE).isoformat().replace("+00:00","Z")
                })
                break

            if msg.get("action") == "deleted":
                self._handle_deletion(msg["trade_id"].strip().upper())
            elif "validated_trade" in msg and "detected_at" in msg:
                self._handle_upsert(
                    trade_id        = msg["trade_id"].strip().upper(),
                    validated_trade = msg["validated_trade"],
                    detected_at     = msg["detected_at"]
                )
            else:
                log_event({
                    "worker":  self.name,
                    "action":  "unknown_message",
                    "message": msg
                })

    def _handle_upsert(self, trade_id: str, validated_trade: dict, detected_at: str):
        ledger = self._load_json(self.ledger_path)

        incoming_norm = {k: validated_trade[k] for k in validated_trade
                         if k not in ("created_at", "updated_at")}

        incoming_norm.setdefault("entry_price", None)
        incoming_norm.setdefault("exit_price",  None)
        
        if trade_id not in ledger:
            # New record
            record = incoming_norm.copy()
            record["created_at"] = detected_at
            record["updated_at"] = detected_at
            ledger[trade_id]      = record
            self._atomic_write_file(self.ledger_path, ledger)
            log_event({
                "worker": self.name,
                "action": "upsert_new",
                "trade_id": trade_id,
                "timestamp": detected_at
            })
            return

        existing      = ledger[trade_id]
        existing_norm = {k: existing[k] for k in existing
                         if k not in ("created_at", "updated_at")}

        if incoming_norm == existing_norm:
            log_event({
                "worker": self.name,
                "action": "upsert_skip",
                "trade_id": trade_id,
                "timestamp": detected_at
            })
            return

        # Merge updates
        updated = existing.copy()
        updated.update(incoming_norm)
        updated["created_at"] = existing["created_at"]
        updated["updated_at"] = detected_at

        ledger[trade_id] = updated
        self._atomic_write_file(self.ledger_path, ledger)
        log_event({
            "worker": self.name,
            "action": "upsert_update",
            "trade_id": trade_id,
            "timestamp": detected_at
        })

    def _handle_deletion(self, trade_id: str):
        ledger  = self._load_json(self.ledger_path)
        trashed = self._load_json(self.trashed_path)

        if trade_id in ledger:
            trashed[trade_id] = ledger[trade_id]
            del ledger[trade_id]

            self._atomic_write_file(self.trashed_path, trashed)
            self._atomic_write_file(self.ledger_path, ledger)
            log_event({
                "worker": self.name,
                "action": "delete",
                "trade_id": trade_id,
                "deleted_at": datetime.now(TIMEZONE).isoformat().replace("+00:00","Z")
            })
        else:
            log_event({
                "worker": self.name,
                "action": "delete_missing",
                "trade_id": trade_id
            })

    def _load_json(self, path: str) -> dict:
        for _ in range(self.read_retries):
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                time.sleep(self.retry_delay)
        # final attempt
        with open(path, "r") as f:
            return json.load(f)

    def _atomic_write_file(self, path: str, data: dict):
        dirpath  = os.path.dirname(path) or "."
        basename = os.path.basename(path)
        fd, tmp   = tempfile.mkstemp(prefix=basename, suffix=".tmp", dir=dirpath)
        try:
            with os.fdopen(fd, "w") as tmp_file:
                json.dump(data, tmp_file, indent=2)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())

            os.replace(tmp, path)

            dir_fd = os.open(dirpath, os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception as e:
        # Log and continue; prevents worker crash on bad data
            log_event({
                "worker": self.name,
                "action": "atomic_write_failed",
                "path": path,
                "error": str(e)
            })
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    mp_manager = mp.Manager()
    ledger_q   = mp_manager.Queue()

    # start directory watcher
    dir_p = DirectoryMonitorProcess(TRADE_INPUT_DIR, ledger_q)
    ledger_p = LedgerUpdaterProcess(ledger_q, 
                                    ledger_path="trade_ledger.json",
                                    trashed_path="trashed_trades.json")
    fetch_p = FetchTickerProcess()  # debug_interval=30 for fetching every 30 seconds
    
    for p in (dir_p, ledger_p, fetch_p):
        p.start()

    # handle shutdown
    def _shutdown(signum, frame):
        log_event({"worker": "main_process", "action": "shutdown_initiated", "trade_id": None})
        dir_p.terminate()
        # fetch_p.terminate()
        ledger_q.put(None)
        ledger_p.terminate()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log_event({"worker": "main_process", "action": "idling", "trade_id": None})
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        # wait for all to clean up
        dir_p.join()
        # fetch_p.join()
        ledger_p.join()
        log_event({"worker": "main_process", "action": "all_processes_exited", "trade_id": None})
        sys.exit(0)

if __name__ == "__main__":
    main()
