# -*- coding: utf-8 -*-
import os
import json
import time
import csv
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import websocket

# ------------ PATHS / ENV ------------
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

DB_PATH = os.getenv("DB_PATH", str(DATA_DIR / "gip_live.db"))
TRADEHISTORY_CSV = os.getenv("TRADEHISTORY_CSV", str(ROOT / "tradehistory_channel.csv"))

EPIAS_USER = os.getenv("EPIAS_USER", "BTHNLGNMOSEDAS")
EPIAS_PASS = os.getenv("EPIAS_PASS", "Bb250512.")

CAS_URL = "https://cas.epias.com.tr/cas/v1/tickets?format=text"
GUNICI_API_URL = "https://gunici.epias.com.tr/gunici-service/rest/v1/user/info"
ALL_CHANNELS = ["TradeHistoryChannel"]

# ------------ LOG ------------
logging.basicConfig(
    filename=str(ROOT / "tradehistory_ws.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ------------ STATE ------------
trade_history = {}  # {contract: [(ts, price, qty), ...]}  (yalnızca son 1 saat)
CSV_HEADER = ["contractName", "time", "price", "quantity", "region", "AOF_last_1h"]

# ------------ DB INIT ------------
DDL_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contractName TEXT NOT NULL,
  time TEXT,                 -- ISO-8601 (WS 'time' alanı)
  price REAL NOT NULL,
  quantity REAL NOT NULL,
  region TEXT,
  snapshot_ts TEXT NOT NULL, -- ingest zamanı (localtime)
  aof_1h REAL,               -- o anki son 1 saat AOF (isteğe bağlı)
  UNIQUE(contractName, time, price, quantity) ON CONFLICT IGNORE
);
"""
IDX1 = "CREATE INDEX IF NOT EXISTS idx_trades_cn_time ON trades(contractName, time);"
IDX2 = "CREATE INDEX IF NOT EXISTS idx_trades_snap ON trades(snapshot_ts);"

def ensure_db(reset: bool = False):
    if reset and os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    con = sqlite3.connect(DB_PATH, timeout=30)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute(DDL_TRADES)
        con.execute(IDX1)
        con.execute(IDX2)
        con.commit()
    finally:
        con.close()

# ------------ CSV ------------
def ensure_csv_header(path: str):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADER)

def append_trade_csv(trade: dict, aof_1h: float):
    ensure_csv_header(TRADEHISTORY_CSV)
    with open(TRADEHISTORY_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            trade.get("contractName"),
            trade.get("time"),
            trade.get("price"),
            trade.get("quantity"),
            trade.get("region"),
            round(aof_1h, 2) if aof_1h is not None else ""
        ])

# ------------ DB WRITE ------------
def insert_trade_db(trade: dict, aof_1h: float | None):
    """
    UPSERT (IGNORE) ile yinelenen mesajları yutuyoruz.
    Unique anahtar: (contractName, time, price, quantity)
    """
    snapshot_ts = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(DB_PATH, timeout=30) as con:
        con.execute(
            """
            INSERT OR IGNORE INTO trades
            (contractName, time, price, quantity, region, snapshot_ts, aof_1h)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade.get("contractName"),
                trade.get("time"),
                float(trade.get("price")),
                float(trade.get("quantity")),
                trade.get("region"),
                snapshot_ts,
                float(aof_1h) if aof_1h is not None else None,
            ),
        )
        con.commit()

# ------------ AOF(1h) HESAP ------------
def update_last_hour_memory(contract: str, ts: pd.Timestamp, price: float, qty: float) -> float:
    """
    Bellekte sadece son 1 saatin işlemlerini tutar ve AOF(1h) döndürür.
    """
    arr = trade_history.get(contract, [])
    arr.append((ts, price, qty))
    # sadece son 1 saat
    cutoff = ts - pd.Timedelta(hours=1)
    arr = [(t, p, q) for (t, p, q) in arr if t >= cutoff]
    trade_history[contract] = arr

    tot_amt = sum(p * q for (_, p, q) in arr)
    tot_q = sum(q for (_, _, q) in arr)
    return (tot_amt / tot_q) if tot_q else price

# ------------ WS HANDLERS ------------
def append_trade(trade: dict):
    """
    Bir TradeHistoryChannel mesajını işler:
    - time parse
    - 1h AOF hesap
    - CSV yaz
    - DB yaz (UPSERT)
    """
    contract = trade.get("contractName")
    tstr = trade.get("time")
    price = float(trade.get("price"))
    quantity = float(trade.get("quantity"))

    ts = pd.to_datetime(tstr, errors="coerce")
    if pd.isna(ts):
        # Zaman bozuksa ingest zamanını uygula (yine de DB unique çatışmasını azaltır)
        ts = pd.Timestamp.now()
        trade["time"] = ts.isoformat(timespec="seconds")
    else:
        # normalize ISO string
        trade["time"] = ts.isoformat(timespec="seconds")

    aof_1h = update_last_hour_memory(contract, ts, price, quantity)

    # CSV
    append_trade_csv(trade, aof_1h)
    # DB
    insert_trade_db(trade, aof_1h)

def on_message(ws, message):
    try:
        logging.info(f"[MSG] {message[:180]} ...")
        data = json.loads(message)
        if data.get("eventType") == "TradeHistoryChannel":
            trade = data.get("body", {})
            # zorunlu alanlar
            if not all(k in trade for k in ("contractName", "time", "price", "quantity")):
                return
            # tip dönüşümleri
            try:
                trade["price"] = float(trade["price"])
                trade["quantity"] = float(trade["quantity"])
            except Exception:
                return
            append_trade(trade)
    except Exception as e:
        logging.error(f"on_message error: {e}")

def on_error(ws, error):
    logging.error(f"WS error: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.warning(f"WS closed: {close_status_code} {close_msg}")

def on_open(ws):
    logging.info("WS opened and listening...")

def ws_thread(ws_url):
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

# ------------ EPİAŞ AUTH / WS URL ------------
def get_tgt():
    data = {"username": EPIAS_USER, "password": EPIAS_PASS}
    try:
        resp = requests.post(CAS_URL, data=data, timeout=10)
        if resp.status_code == 201:
            logging.info("TGT alındı.")
            return resp.text
        logging.error(f"TGT alınamadı: {resp.text}")
        return None
    except Exception as e:
        logging.error(f"TGT hata: {e}")
        return None

def get_websocket_url_and_jwt(tgt):
    headers = {"TGT": tgt, "Accept": "application/json"}
    try:
        resp = requests.get(GUNICI_API_URL, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            ws_url_raw = data["body"]["content"]["webSocketDto"]["url"]
            if not ws_url_raw.startswith("/gunici-service"):
                ws_url_raw = "/gunici-service" + ws_url_raw
            return ws_url_raw
        logging.error(f"JWT/WS URL alınamadı: {resp.text}")
        return None
    except Exception as e:
        logging.error(f"JWT/WS hata: {e}")
        return None

def get_fresh_ws_url():
    tgt = get_tgt()
    if not tgt:
        return None
    ws_url_raw = get_websocket_url_and_jwt(tgt)
    if not ws_url_raw:
        return None
    event_params = "".join([f"&event={c}" for c in ALL_CHANNELS])
    if "?" in ws_url_raw:
        return f"wss://gunici.epias.com.tr{ws_url_raw}{event_params}"
    else:
        return f"wss://gunici.epias.com.tr{ws_url_raw}?{event_params[1:]}"

# ------------ RUN LOOP ------------
def keep_running():
    while True:
        try:
            ws_url = get_fresh_ws_url()
            if not ws_url:
                logging.warning("WS URL alınamadı, 60 sn bekleniyor...")
                time.sleep(60)
                continue
            logging.info(f"WS başlatılıyor: {ws_url}")
            ws_thread(ws_url)
        except Exception as e:
            logging.error(f"Ana döngü hatası: {e}")
        logging.warning("Bağlantı koptu, 60 sn sonra tekrar denenecek...")
        time.sleep(60)

if __name__ == "__main__":
    # İlk çalıştırmada yeni, temiz DB istersen:
    # ensure_db(reset=True)
    ensure_db(reset=False)
    print(f"DB: {DB_PATH}")
    print("TradeHistory WS ingest başlıyor...")
    keep_running()
