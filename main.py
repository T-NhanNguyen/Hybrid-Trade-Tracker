#!/usr/bin/env python3
import os
import sys
import time
import json
import tempfile
import hashlib
import signal
import logging
import multiprocessing as mp

from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from multiprocessing import Process, Queue

import pandas as pd
import yfinance as yf
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent, FileDeletedEvent
from pydantic import ValidationError
from shared_schemas import TradeInputModel

# ─── Configuration ─────────────────────────────────────────────────────────────

TRADE_INPUT_DIR     = Path(os.getenv("TRADE_INPUT_DIR", "Trade Speculations"))
TRADES_CSV          = Path(os.getenv("TRADES_CSV", "trades.csv"))
TIME_SERIES_DIR     = Path(os.getenv("TIME_SERIES_DIR", "time_series"))
LEDGER_PATH         = Path(os.getenv("LEDGER_PATH", "trade_ledger.json"))
TRASHED_PATH        = Path(os.getenv("TRASHED_PATH", "trashed_trades.json"))
FETCH_INTERVAL      = int(os.getenv("FETCH_INTERVAL_SECONDS", "300"))  # seconds between fetch cycles
DEBOUNCE_SECONDS    = float(os.getenv("DEBOUNCE_SECONDS", ".5"))
ALLOWED_EXTS        = {".json"}
CACHE_FILENAME      = ".dir_cache.json"

# ─── Utilities ─────────────────────────────────────────────────────────────────

def current_utc_timestamp() -> str:
    # use a timezone-aware datetime right off the bat
    # return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    # return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    # new — ISO8601 to the second:
    return datetime.now(timezone.utc) \
                 .isoformat(timespec='seconds') \
                 .replace("+00:00","Z")
    
    # human-friendly with UTC label. NOT ISO 8601 STANDARD
    # return datetime.now(timezone.utc) \
    #              .strftime("%Y-%m-%d %H:%M:%S UTC")

def log_event(event: dict):
    """
    Stamp and emit a uniform JSON log:
      - worker   : string
      - action   : string
      - trade_id : optional
      + timestamp (added here)
    """
    event.setdefault("timestamp", current_utc_timestamp())
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
                "timestamp": current_utc_timestamp()
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
            "detected_at": current_utc_timestamp()
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
            except ValidationError:
                log_event({
                    "worker": self.name,
                    "action": "validation_error",
                    "trade_id": tid
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
                    "detected_at": current_utc_timestamp()
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

# ─── Fetch Ticker Process ───────────────────────────────────────────────────────

class YFinanceWorker:
    """
    Fetches market data for active trades and appends to per‐ticker CSV.
    """
    def __init__(self, trades_csv: Path, output_dir: Path, rsi_period: int = 14):
        self.trades_csv = trades_csv
        self.output_dir = output_dir
        self.rsi_period = rsi_period
        self.name       = "fetch_ticker_process"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        logging.getLogger().setLevel(logging.INFO)

    def calculate_rsi(self, series: pd.Series, period: int) -> float:
        delta  = series.diff().dropna()
        gains  = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_g  = gains.rolling(period).mean().iloc[-1]
        avg_l  = losses.rolling(period).mean().iloc[-1]
        if avg_l == 0:
            return 100.0
        rs = avg_g / avg_l
        return 100 - (100 / (1 + rs))

    def load_trades(self) -> list[dict]:
        df = pd.read_csv(self.trades_csv, parse_dates=["expiry_date"])
        mask = df.status.isin(["Active", "Spectating"]) & ~df.manual_closed
        return df[mask].to_dict(orient="records")

    def fetch_trade_data(self, trade: dict) -> dict:
        ticker = trade["ticker"]
        y      = yf.Ticker(ticker)
        hist   = y.history(period="5d", interval="1m")
        latest = float(hist["Close"].iloc[-1])
        sma    = float(hist["Close"].rolling(50).mean().iloc[-1])
        rsi    = self.calculate_rsi(hist["Close"], self.rsi_period)

        # choose expiry
        exp_dates = y.options
        chosen   = trade["expiry_date"].strftime("%Y-%m-%d")
        if chosen not in exp_dates:
            chosen = exp_dates[0]
        opt_chain = y.option_chain(chosen)
        return {
            "snapshot_timestamp": current_utc_timestamp(),
            "price": latest,
            "sma": sma,
            "rsi": rsi,
            # omit raw DataFrames from ledger
        }

    def append_to_csv(self, trade: dict, data: dict):
        ticker   = trade["ticker"]
        out_file = self.output_dir / f"{ticker}.csv"
        tmp_file = out_file.with_suffix(".csv.tmp")

        row = {"trade_id": trade["trade_id"], **data}
        new_df = pd.DataFrame([row])

        if out_file.exists():
            old = pd.read_csv(out_file, parse_dates=["snapshot_timestamp"])
            mask = ~((old["trade_id"] == row["trade_id"]) &
                     (old["snapshot_timestamp"].astype(str) == row["snapshot_timestamp"]))
            df = pd.concat([old[mask], new_df], ignore_index=True)
        else:
            df = new_df

        with open(tmp_file, "w", newline="") as f:
            df.to_csv(f, index=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp_file, out_file)
        logging.info(f"[{self.name}] Upserted {ticker} @ {data['snapshot_timestamp']}")

    def is_expired(self, trade: dict) -> bool:
        return datetime.utcnow().date() > trade["expiry_date"].date()

    def should_auto_close(self, trade: dict) -> bool:
        return (datetime.utcnow() + timedelta(hours=3)).date() > trade["expiry_date"].date()

    def run_cycle(self):
        trades = self.load_trades()
        for t in trades:
            data = self.fetch_trade_data(t)
            self.append_to_csv(t, data)
            # expiry handling
            if self.is_expired(t):
                logging.info(f"[{self.name}] Trade {t['trade_id']} expired.")
            elif self.should_auto_close(t):
                logging.info(f"[{self.name}] Auto-closing {t['trade_id']}.")

class FetchTickerProcess(mp.Process):
    """
    Wraps YFinanceWorker into a long‐running MP process.
    """
    def __init__(self, trades_csv: Path, output_dir: Path, interval: int):
        super().__init__(name="fetch_ticker_process", daemon=True)
        self.worker   = YFinanceWorker(trades_csv, output_dir)
        self.interval = interval

    def run(self):
        log_event({"worker": self.name, "action": "started", "trade_id": None})
        try:
            while True:
                self.worker.run_cycle()
                time.sleep(self.interval)
        except KeyboardInterrupt:
            pass
        finally:
            log_event({"worker": self.name, "action": "exited", "trade_id": None})

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
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        })

        while True:
            try:
                msg = self.ledger_queue.get()    # <— may raise KeyboardInterrupt on Ctrl+C
            except KeyboardInterrupt:
                log_event({
                    "worker":    self.name,
                    "action":    "shutdown_triggered",
                    "timestamp":  datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
                })
                break
            if msg is None:
                log_event({
                    "worker":    self.name,
                    "action":    "shutdown_received",
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
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
                "deleted_at": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
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
    dir_p.start()

    # start ledger updater
    ledger_p = LedgerUpdaterProcess(ledger_q, 
                                    ledger_path="trade_ledger.json",
                                    trashed_path="trashed_trades.json")
    ledger_p.start()

    # # start ticker fetcher
    # fetch_p = FetchTickerProcess(TRADES_CSV, TIME_SERIES_DIR, FETCH_INTERVAL)
    # fetch_p.start()

    # handle shutdown
    def _shutdown(signum, frame):
        log_event({"worker": "main_process", "action": "shutdown_initiated", "trade_id": None})
        dir_p.terminate()
        # fetch_p.terminate()
        # signal ledger to exit
        ledger_q.put(None)
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
