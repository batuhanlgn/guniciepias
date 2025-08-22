import requests
import json
import websocket
import time
from datetime import datetime
import os
import csv
import logging
from pathlib import Path
from dotenv import load_dotenv

# --- LOG AYARI ---
logging.basicConfig(
    filename="gunici_ws.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

BOARDINFO_CSV = os.getenv("BOARDINFO_CSV", str(ROOT / "boardinfo_history.csv"))

def extract_and_write_boardinfo(raw_message):
    try:
        data = json.loads(raw_message)
        body = data.get("body", {})
        board = body.get("boardInformation", None)
        best_buy = body.get("bestBuyPrice")
        best_sell = body.get("bestSellPrice")
        if board:
            file_exists = os.path.isfile(BOARDINFO_CSV)
            
            # Add debug logging
            logging.info(f"Writing to CSV: {BOARDINFO_CSV}")
            logging.info(f"Contract: {body.get('name')}, MCP: {board.get('mcp')}")
            
            with open(BOARDINFO_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow([
                        "contractName", "time", "averagePrice", "minPrice", "maxPrice",
                        "mcp", "lastPrice", "total", "volume", "bestBuyPrice", "bestSellPrice"
                    ])
                writer.writerow([
                    body.get("name"),
                    body.get("deliveryDateStart", data.get("time", "")),
                    board.get("averagePrice"),
                    board.get("minPrice"),
                    board.get("maxPrice"),
                    board.get("mcp"),
                    board.get("lastPrice"),
                    board.get("total"),
                    board.get("volume"),
                    best_buy,
                    best_sell
                ])
    except Exception as e:
        logging.error(f"BoardInfo CSV kaydetme hatası: {e}")
        logging.exception("Full traceback:")  # This will log the full stack trace

def on_message(ws, message):
    try:
        ts = datetime.now().strftime("%H:%M:%S")
        logging.info(f"[{ts}] WS Message: {message[:200]} ...")
        extract_and_write_boardinfo(message)
    except Exception as e:
        logging.error(f"Mesaj işleme hatası: {e}")

def on_error(ws, error):
    logging.error(f"WebSocket hata: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.warning(f"Bağlantı kapandı: {close_status_code} {close_msg}")

def on_open(ws):
    logging.info("WebSocket bağlantısı açıldı ve dinleniyor...")

def ws_thread(ws_url):
    """Bağlantıyı koparsa/timeout yerse/exception alırsa çıkıp üst döngüden tekrar çağrılır."""
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

# --- Kullanıcı Bilgileri ve Ayarlar ---
EKYS_USERNAME = "BTHNLGNMOSEDAS"
EKYS_PASSWORD = "Bb250512."
CAS_URL = "https://cas.epias.com.tr/cas/v1/tickets?format=text"
GUNICI_API_URL = "https://gunici.epias.com.tr/gunici-service/rest/v1/user/info"

ALL_CHANNELS = [
    "ContractBoardMessage"
]

def get_tgt():
    data = {"username": EKYS_USERNAME, "password": EKYS_PASSWORD}
    try:
        resp = requests.post(CAS_URL, data=data)
        if resp.status_code == 201:
            tgt = resp.text
            logging.info("TGT başarıyla alındı.")
            return tgt
        else:
            logging.error(f"TGT alınamadı: {resp.text}")
            return None
    except Exception as e:
        logging.error(f"TGT alınırken hata: {e}")
        return None

def get_websocket_url_and_jwt(tgt):
    headers = {"TGT": tgt, "Accept": "application/json"}
    try:
        resp = requests.get(GUNICI_API_URL, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            ws_url_raw = data["body"]["content"]["webSocketDto"]["url"]
            if not ws_url_raw.startswith("/gunici-service"):
                ws_url_raw = "/gunici-service" + ws_url_raw
            logging.info(f"WebSocketDto url: {ws_url_raw}")
            return ws_url_raw
        else:
            logging.error(f"JWT/Websocket URL alınamadı: {resp.text}")
            return None
    except Exception as e:
        logging.error(f"JWT/Websocket URL alınırken hata: {e}")
        return None

def get_fresh_ws_url():
    tgt = get_tgt()
    if not tgt:
        return None
    ws_url_raw = get_websocket_url_and_jwt(tgt)
    if not ws_url_raw:
        return None
    event_params = "".join([f"&event={c}" for c in ALL_CHANNELS])
    if event_params:
        if "?" in ws_url_raw:
            ws_url = f"wss://gunici.epias.com.tr{ws_url_raw}{event_params}"
        else:
            ws_url = f"wss://gunici.epias.com.tr{ws_url_raw}?{event_params[1:]}"
    else:
        ws_url = f"wss://gunici.epias.com.tr{ws_url_raw}"
    return ws_url

def main_keep_alive():
    """Sonsuz döngü: koparsa veya TGT/JWT expire olursa tekrar bağlanır."""
    while True:
        try:
            ws_url = get_fresh_ws_url()
            if not ws_url:
                logging.warning("WS URL alınamadı, 1 dk sonra tekrar denenecek.")
                time.sleep(60)
                continue
            logging.info("WS başlatılıyor...")
            ws_thread(ws_url)
        except Exception as e:
            logging.error(f"Main döngüde hata: {e}")
        logging.warning("Bağlantı koptu veya hata oluştu, 60 saniye bekleniyor...")
        time.sleep(60)

if __name__ == "__main__":
    print("Başladı...")
    main_keep_alive()

from pathlib import Path
csv_path = Path(r"c:\Users\batuh\OneDrive\Masaüstü\Projeler\guniciepias\boardinfo_history.csv")
db_folder = Path(r"c:\Users\batuh\OneDrive\Masaüstü\Projeler\guniciepias\data")
db_folder.mkdir(parents=True, exist_ok=True)