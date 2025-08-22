import os
import sqlite3
import threading
import requests
import logging
from pathlib import Path
from dotenv import load_dotenv

# .env yükle
ENV_CANDIDATES = [os.path.join(os.getcwd(), ".env"),
                  os.path.join(os.path.dirname(__file__), ".env")]
for _p in ENV_CANDIDATES:
    if os.path.exists(_p):
        load_dotenv(_p)
        break

# Kimlik ve yollar
EKYS_USERNAME = os.getenv("EKYS_USERNAME") or os.getenv("EPYS_USER") or os.getenv("EKYS_USER")
EKYS_PASSWORD = os.getenv("EKYS_PASSWORD") or os.getenv("EPYS_PASS") or os.getenv("EPYS_PASSWORD")
CAS_URL = "https://cas.epias.com.tr/cas/v1/tickets?format=text"
GUNICI_API_URL = "https://gunici.epias.com.tr/gunici-service/rest/v1/user/info"

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = os.getenv("DB_PATH", str(DATA_DIR / "epias_gip.db"))

def setup_logger(filename: str, level=logging.INFO):
    logging.basicConfig(filename=filename, level=level,
                        format="%(asctime)s [%(levelname)s] %(message)s")

# --- SQLite (tek bağlantı + kilit güvenli) ---
_DB_LOCK = threading.Lock()
_DB_CONN = None

def _open_db():
    global _DB_CONN
    if _DB_CONN is None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=60, isolation_level=None, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=60000;")
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS boardinfo (
          contractName TEXT NOT NULL,
          time         TEXT NOT NULL,
          averagePrice REAL, minPrice REAL, maxPrice REAL,
          mcp REAL, lastPrice REAL, total REAL, volume REAL,
          bestBuyPrice REAL, bestSellPrice REAL,
          PRIMARY KEY(contractName, time)
        );
        CREATE INDEX IF NOT EXISTS ix_boardinfo_time ON boardinfo(time);

        CREATE TABLE IF NOT EXISTS trades (
          contractName TEXT NOT NULL,
          time         TEXT NOT NULL,
          tradeId      TEXT,
          price        REAL,
          quantity     REAL,
          region       TEXT,
          PRIMARY KEY(contractName, time, tradeId)
        );
        CREATE INDEX IF NOT EXISTS ix_trades_cn_time ON trades(contractName, time);
        """)
        _DB_CONN = conn
    return _DB_CONN

def get_db_path() -> str:
    return DB_PATH

# --- Upsert / Insert ---
def upsert_boardinfo(contractName: str, time_iso: str,
                     averagePrice=None, minPrice=None, maxPrice=None,
                     mcp=None, lastPrice=None, total=None, volume=None,
                     bestBuyPrice=None, bestSellPrice=None):
    # Zorunlu alan kontrolü
    if not contractName or not time_iso:
        return  # sessizce atla

    sql = """
    INSERT INTO boardinfo
    (contractName,time,averagePrice,minPrice,maxPrice,mcp,lastPrice,total,volume,bestBuyPrice,bestSellPrice)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(contractName,time) DO UPDATE SET
      averagePrice=excluded.averagePrice,
      minPrice=excluded.minPrice,
      maxPrice=excluded.maxPrice,
      mcp=excluded.mcp,
      lastPrice=excluded.lastPrice,
      total=excluded.total,
      volume=excluded.volume,
      bestBuyPrice=excluded.bestBuyPrice,
      bestSellPrice=excluded.bestSellPrice
    """
    vals = (contractName, time_iso, averagePrice, minPrice, maxPrice, mcp,
            lastPrice, total, volume, bestBuyPrice, bestSellPrice)

    with _DB_LOCK:
        conn = _open_db()
        backoff = 0.2
        for _ in range(10):
            try:
                conn.execute(sql, vals)   # BEGIN kullanma
                conn.commit()
                break
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "locked" in msg or "transaction" in msg:
                    try: conn.rollback()
                    except: pass
                    import time as _t
                    _t.sleep(backoff)
                    backoff = min(backoff * 1.8, 3.0)
                    continue
                raise



def insert_trade(contractName: str, time_iso: str, price, quantity, region=None, tradeId=None):
    sql = "INSERT OR IGNORE INTO trades (contractName,time,tradeId,price,quantity,region) VALUES (?,?,?,?,?,?)"
    vals = (contractName, time_iso, tradeId, price, quantity, region)

    with _DB_LOCK:
        conn = _open_db()
        backoff = 0.2
        for _ in range(10):
            try:
                conn.execute(sql, vals)
                conn.commit()
                break
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "locked" in msg or "transaction" in msg:
                    try: conn.rollback()
                    except: pass
                    import time as _t
                    _t.sleep(backoff)
                    backoff = min(backoff * 1.8, 3.0)
                    continue
                raise

# --- CAS / WS URL ---
def get_tgt():
    data = {"username": EKYS_USERNAME, "password": EKYS_PASSWORD}
    try:
        resp = requests.post(CAS_URL, data=data, timeout=20)
        resp.raise_for_status()
        return resp.text.strip()
    except Exception as e:
        logging.error(f"TGT alınırken hata: {e}")
        return None

def get_websocket_url_and_jwt(tgt):
    headers = {"TGT": tgt, "Accept": "application/json"}
    try:
        resp = requests.get(GUNICI_API_URL, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        ws_url_raw = data["body"]["content"]["webSocketDto"]["url"]
        if not ws_url_raw.startswith("/gunici-service"):
            ws_url_raw = "/gunici-service" + ws_url_raw
        return ws_url_raw
    except Exception as e:
        logging.error(f"JWT/Websocket URL alınırken hata: {e}")
        return None

def get_fresh_ws_url(ALL_CHANNELS):
    tgt = get_tgt()
    if not tgt: return None
    ws_url_raw = get_websocket_url_and_jwt(tgt)
    if not ws_url_raw: return None
    event_params = "".join([f"&event={c}" for c in ALL_CHANNELS])
    if event_params:
        if "?" in ws_url_raw:
            return f"wss://gunici.epias.com.tr{ws_url_raw}{event_params}"
        else:
            return f"wss://gunici.epias.com.tr{ws_url_raw}?{event_params[1:]}"
    return f"wss://gunici.epias.com.tr{ws_url_raw}"
