# dashboard.py
import os
import math
import time
import sqlite3
import io
from pathlib import Path
from datetime import datetime
from typing import Optional

# Çevre değişkenlerini yükle
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv mevcut değil

# Kimlik doğrulama
from auth import (
    authenticate_user, get_user_permissions, 
    approve_user, get_pending_users, create_user_by_admin
)

# Veri işleme
import numpy as np
import pandas as pd

# Excel işleme
from openpyxl import Workbook
import xlsxwriter

# Görselleştirme
import plotly.express as px

# Web/API
import requests

import streamlit as st
import time
# Otomatik yenileme import kaldırıldı

# ======================== .env / Dosya Yolları ========================
ROOT = Path(__file__).resolve().parent

DB_PATH = os.getenv("DB_PATH", str(ROOT / "data" / "gip_live.db"))
BOARDINFO_CSV = os.getenv("BOARDINFO_CSV", str(ROOT / "boardinfo_history.csv"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_IDS = [int(x) for x in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if x.strip()]

def save_telegram_config_to_env(token, chat_ids):
    """Telegram ayarlarını .env dosyasına kaydet"""
    try:
        env_file = ROOT / ".env"
        env_content = ""
        
        # Mevcut .env dosyasını oku
        if env_file.exists():
            with open(env_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
            # TELEGRAM satırlarını güncelle/ekle
            telegram_token_updated = False
            telegram_chat_ids_updated = False
            
            for i, line in enumerate(lines):
                if line.startswith("TELEGRAM_BOT_TOKEN="):
                    lines[i] = f"TELEGRAM_BOT_TOKEN={token}\n"
                    telegram_token_updated = True
                elif line.startswith("TELEGRAM_CHAT_IDS="):
                    lines[i] = f"TELEGRAM_CHAT_IDS={','.join(map(str, chat_ids))}\n"
                    telegram_chat_ids_updated = True
            
            # Eğer satırlar yoksa ekle
            if not telegram_token_updated:
                lines.append(f"TELEGRAM_BOT_TOKEN={token}\n")
            if not telegram_chat_ids_updated:
                lines.append(f"TELEGRAM_CHAT_IDS={','.join(map(str, chat_ids))}\n")
                
            env_content = ''.join(lines)
        else:
            # Yeni .env dosyası oluştur
            env_content = f"TELEGRAM_BOT_TOKEN={token}\nTELEGRAM_CHAT_IDS={','.join(map(str, chat_ids))}\n"
        
        # .env dosyasını yaz
        with open(env_file, 'w', encoding='utf-8') as f:
            f.write(env_content)
            
        # Çevre değişkenlerini güncelle
        os.environ["TELEGRAM_BOT_TOKEN"] = token
        os.environ["TELEGRAM_CHAT_IDS"] = ','.join(map(str, chat_ids))
        
        return True
    except Exception as e:
        st.error(f"Telegram config kaydetme hatası: {e}")
        return False

# ======================== Streamlit Arayüz Ayarları ========================
st.set_page_config(page_title="GİP Trade Dashboard", page_icon="🚦", layout="wide", initial_sidebar_state="expanded")

# --- Otomatik Yenileme (stabil yol) ---
from streamlit_autorefresh import st_autorefresh

# Her 1000 ms'de (1 sn) bir rerun; sayfayı komple yenilemez, sadece scripti tekrar çalıştırır
refresh_count = st_autorefresh(interval=1000, limit=None, key="gip_dash_autorefresh")


# Sağ üstte küçük sayaç/gösterge
st.markdown(
    f"""
    <div style="position: fixed; top: 10px; right: 10px; background: #00ff00; color: black; padding: 8px 12px; border-radius: 8px; z-index: 9999; font-weight: bold; box-shadow: 0 2px 10px rgba(0,0,0,0.3);">
        🔄 YENİLEME #{refresh_count} • {datetime.now().strftime('%H:%M:%S')}
    </div>
    """,
    unsafe_allow_html=True
)

# ==================== KİMLİK DOĞRULAMA SİSTEMİ ====================
def show_login_page():
    """Sadece giriş sayfasını göster"""
    st.markdown("<h1 style='text-align: center; color: #e7b416;'>🚦 GİP Trade Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: #666;'>Güvenli Giriş Sistemi</h3>", unsafe_allow_html=True)
    
    st.markdown("### 🔑 Giriş Yap")
    with st.form("login_form"):
        username = st.text_input("👤 Kullanıcı Adı", placeholder="kullanici_adi")
        password = st.text_input("🔒 Şifre", type="password", placeholder="********")
        login_submit = st.form_submit_button("🚪 Giriş Yap", use_container_width=True)
        
        if login_submit:
            if username and password:
                success, message = authenticate_user(username, password)
                if success:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.user_permissions = get_user_permissions(username)
                    
                    # Session dosyasını oluştur
                    session_file = ROOT / "data" / "dashboard_session.txt"
                    try:
                        session_file.parent.mkdir(exist_ok=True)
                        with open(session_file, 'w') as f:
                            f.write(f"{username}\n{time.time()}")
                        st.success(f"✅ {message} (Session kaydedildi)")
                    except:
                        st.success(f"✅ {message}")
                    
                    st.rerun()
                else:
                    st.error(f"❌ {message}")
            else:
                st.warning("⚠️ Tüm alanları doldurun")
    
    # Admin bilgisi
    with st.expander("ℹ️ Bilgilendirme"):
        st.info("🔹 **Giriş bilgilerinizi** sistem yöneticisinden alın")

def show_admin_panel():
    """Kullanıcı yönetimi için admin panelini göster"""
    if st.session_state.get('user_permissions', {}).get('user_management', False):
        st.sidebar.markdown("---")
        st.sidebar.markdown("### � Admin Panel")
        
        # Add new user section
        with st.sidebar.expander("➕ Yeni Kullanıcı Ekle"):
            with st.form("add_user_form"):
                new_username = st.text_input("👤 Kullanıcı Adı", placeholder="yeni_kullanici")
                new_password = st.text_input("🔒 Şifre", placeholder="güvenli_şifre")
                user_role = st.selectbox("🎭 Rol", ["user", "admin"])
                add_user_submit = st.form_submit_button("➕ Kullanıcı Ekle")
                
                if add_user_submit:
                    if new_username and new_password:
                        success, message = create_user_by_admin(st.session_state.username, new_username, new_password, user_role)
                        if success:
                            st.success(f"✅ {message}")
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
                    else:
                        st.warning("⚠️ Tüm alanları doldurun")
        
        # Show existing users
        pending_users = get_pending_users()
        if pending_users:
            st.sidebar.warning(f"⏳ {len(pending_users)} onay bekliyor")
            
            with st.sidebar.expander("Bekleyen Kullanıcılar"):
                for user in pending_users:
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        st.text(f"👤 {user['username']}")
                    with col2:
                        if st.button("✅", key=f"approve_{user['username']}"):
                            success, message = approve_user(st.session_state.username, user['username'])
                            if success:
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)
        else:
            st.sidebar.success("✅ Bekleyen kullanıcı yok")

# Check authentication - dosya tabanlı session backup
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# Dosya tabanlı session restore
if not st.session_state.authenticated:
    session_file = ROOT / "data" / "dashboard_session.txt"
    try:
        if session_file.exists():
            with open(session_file, 'r') as f:
                lines = f.read().strip().split('\n')
                if len(lines) >= 2:
                    username = lines[0]
                    timestamp = float(lines[1])
                    
                    # 24 saat kontrolü
                    if time.time() - timestamp < 24 * 60 * 60:
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        from auth import get_user_permissions
                        st.session_state.user_permissions = get_user_permissions(username)
                    else:
                        # Eski session dosyasını sil
                        session_file.unlink()
    except:
        pass

if not st.session_state.get('authenticated', False):
    show_login_page()
    st.stop()

# Show user info in sidebar
st.sidebar.success(f"👋 Hoş geldin, **{st.session_state.username}**!")
permissions = st.session_state.get('user_permissions', {})
role = "👑 Admin" if permissions.get('user_management', False) else "👤 Kullanıcı"
st.sidebar.info(f"**Rol:** {role}")

# Show admin panel
show_admin_panel()

# Logout button - session dosyasını sil
if st.sidebar.button("🚪 Çıkış Yap", use_container_width=True):
    # Session dosyasını sil
    session_file = ROOT / "data" / "dashboard_session.txt"
    try:
        if session_file.exists():
            session_file.unlink()
    except:
        pass
    
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.user_permissions = {}
    st.rerun()

st.sidebar.markdown("---")

# ==================== ANA DASHBOARD ====================
# Otomatik yenileme kaldırıldı - sadece manuel yenileme

st.markdown("""
<style>
.block-container {padding-top: 1.2rem;}
thead tr th { font-size: 1.05em; }
tbody tr td { font-size: 1.04em; }

.live-clock {
    position: absolute;
    right: 2rem;
    top: 1.0rem;
    font-size: 1.1em;
    font-weight: 600;
    color: #e7b416;
    background: #23272f;
    padding: 6px 16px;
    border-radius: 14px;
    box-shadow: 0 2px 12px #0003;
    letter-spacing: 0.04em;
}

.cnwrap {
    display: flex;
    flex-direction: column;
    gap: 6px;
    min-width: 260px;
}

.cnhead {
    display: flex;
    align-items: center;
    justify-content: space-between;
}

.cnname { font-weight: 700; letter-spacing: 0.02em; }
.cntimer { font-size: 0.90em; opacity: 0.95; }

.barwrap {
    width: 100%;
    height: 10px;
    background: #2a2f3a;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: inset 0 0 0 1px #0006;
}

.barfill { height: 100%; }
.bar-green { background: linear-gradient(90deg,#43e97b,#38f9d7); }
.bar-orange { background: linear-gradient(90deg,#ffc371,#ff5f6d); }
.bar-black { background: #000; }

.gap-pos { color: #45ffac; font-weight: 700; }
.gap-neg { color: #ff7575; font-weight: 700; }
.yes { font-size: 1.3em; color: #00ff6e; }
.no { font-size: 1.3em; color: #ff3939; }
</style>
""", unsafe_allow_html=True)

def show_clock():
    # Real-time updating clock
    current_time = datetime.now()
    st.markdown(f"""
    <div class='live-clock'>
        {current_time.strftime('%H:%M:%S')} 
        <small style='opacity:0.7'>({current_time.strftime('%d.%m.%Y')})</small>
    </div>
    """, unsafe_allow_html=True)
show_clock()
st.markdown("## <span style='color:#e7b416;'>GİP Trade Dashboard</span>  <span style='opacity:.6'>🚦</span>", unsafe_allow_html=True)

# ---------------- Sidebar ----------------
st.sidebar.subheader("📊 Görselleştirmeler")
show_charts = st.sidebar.checkbox("Grafikleri Göster", value=False, key="show_charts_main")
show_analytics = st.sidebar.checkbox("Analitikleri Göster", value=False, key="show_analytics_main")


# Checkbox durumlarını kontrol et ve gerekirse temizle
if not show_charts:
    # Grafik ile ilgili tüm session state verilerini temizle
    keys_to_remove = [k for k in st.session_state.keys() if 'chart' in k.lower()]
    for key in keys_to_remove:
        del st.session_state[key]

if not show_analytics:
    # Analitik ile ilgili tüm session state verilerini temizle
    keys_to_remove = [k for k in st.session_state.keys() if 'analytics' in k.lower()]
    for key in keys_to_remove:
        del st.session_state[key]

# Alarm Settings - Permission based
st.sidebar.markdown("### 🚨 Fiyat Alarmları")
user_permissions = st.session_state.get('user_permissions', {})

if user_permissions.get('visual_alarms', False):
    alarm_enabled = st.sidebar.checkbox("Alarm Sistemi Aktif", value=False, key="alarm_enabled")
    
    if alarm_enabled:
        st.sidebar.info("🔔 Görsel alarmlar aktif")
        if user_permissions.get('telegram', False):
            st.sidebar.success("📱 Telegram bildirimleri aktif")
        else:
            st.sidebar.info("📱 Telegram bildirimleri: Sadece admin")
else:
    st.sidebar.warning("⚠️ Alarm yetkiniz yok")
    alarm_enabled = False

# Telegram Settings - Admin only
if user_permissions.get('telegram', False):
    st.sidebar.markdown("### 📱 Telegram Ayarları")
    
    with st.sidebar.expander("⚙️ Telegram Konfigürasyonu"):
        current_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        current_chat_ids = os.getenv("TELEGRAM_CHAT_IDS", "")
        
        st.write("**Mevcut Ayarlar:**")
        st.write(f"Token: {'✅ Ayarlı' if current_token else '❌ Yok'}")
        st.write(f"Chat IDs: {current_chat_ids if current_chat_ids else '❌ Yok'}")
        
        with st.form("telegram_config"):
            new_token = st.text_input("Bot Token:", value=current_token, type="password")
            new_chat_ids = st.text_input("Chat ID'ler (virgülle ayır):", value=current_chat_ids)
            
            if st.form_submit_button("💾 Kaydet"):
                if new_token and new_chat_ids:
                    try:
                        chat_id_list = [int(x.strip()) for x in new_chat_ids.split(",") if x.strip()]
                        if save_telegram_config_to_env(new_token, chat_id_list):
                            st.success("✅ Telegram ayarları kaydedildi!")
                            st.rerun()
                        else:
                            st.error("❌ Kaydetme hatası!")
                    except ValueError:
                        st.error("❌ Chat ID'ler sadece sayı olmalı!")
                else:
                    st.error("❌ Tüm alanları doldurun!")
    
    # Telegram Alert Settings
    with st.sidebar.expander("🚨 Alarm Eşikleri"):
        gap_threshold = st.number_input(
            "GAP Eşiği (TL) - Mutlak Değer", 
            min_value=0.1, 
            max_value=1000.0, 
            value=5.0, 
            step=0.1,
            key="telegram_gap_threshold"
        )
        
        alert_interval = st.number_input(
            "Bildirim Aralığı (dakika)", 
            min_value=1, 
            max_value=1440,  # 24 saat
            value=30,  # Varsayılan 30 dakika
            step=1,
            key="telegram_alert_interval"
        )
        
        st.info(f"🔔 {gap_threshold} TL üzeri GAP'ler için {alert_interval} dk'da bir bildirim")
        
# Time filter section
st.sidebar.markdown("### ⏱️ Zaman Filtresi")

# Time filter selection with unique key
zaman = st.sidebar.radio(
    "Zaman dilimi", 
    ["Tümü", "Sabah (01-08)", "Öğle (09-16)", "Akşam (17-24)", "Özel"], 
    index=0, 
    key="time_period_filter"
)

# Custom time range inputs with unique keys
if zaman == "Özel":
    cs = st.sidebar.number_input(
        "Başlangıç saati",
        min_value=0,
        max_value=24,
        value=0,
        step=1,
        key="custom_time_start"
    )
    ce = st.sidebar.number_input(
        "Bitiş saati",
        min_value=0,
        max_value=24,
        value=24,
        step=1,
        key="custom_time_end"
    )
else:
    cs = ce = None

# Add checkbox for showing closed contracts with unique key
show_closed = st.sidebar.checkbox("Kapanmış Kontratları Göster", value=False, key="show_closed_main")

# Session state initialization for telegram (keep backend functionality)
if "telegram_running" not in st.session_state:
    st.session_state.telegram_running = False
    st.session_state.last_notify = {}
    st.session_state.first_run_alarms_sent = False  # İlk çalıştırma kontrolü

# ======================== Helper Functions ========================
def norm_cn(x) -> str:
    return (str(x).strip().upper()) if pd.notna(x) else ""

def parse_cn_datetime(cn: str):
    try:
        if cn.startswith("PH") and len(cn) >= 10 and cn[2:10].isdigit():
            yy = int(cn[2:4]); mm = int(cn[4:6]); dd = int(cn[6:8]); hh = int(cn[8:10])
            return datetime(2000+yy, mm, dd), hh
    except Exception:
        pass
    return None, None

def contract_cutoff(cn: str):
    d, hh = parse_cn_datetime(cn)
    if d is None: 
        return None
    cutoff_hour = max(0, hh - 1)
    return datetime(d.year, d.month, d.day, cutoff_hour, 0, 0)

def remaining_info(cn: str):
    now = datetime.now()
    co = contract_cutoff(cn)
    if co is None or now >= co:
        return ("Kapalı", 100, "bar-black")
    day0 = datetime(co.year, co.month, co.day, 0, 0, 0)
    total = (co - day0).total_seconds()
    elapsed = (now - day0).total_seconds()
    pct = int(np.clip((elapsed/total)*100, 0, 100)) if total > 0 else 0
    rem = (co - now).total_seconds()
    hh = int(rem // 3600); mm = int((rem % 3600)//60)
    lbl = f"{hh}sa {mm}dk kaldı" if hh > 0 else f"{mm}dk kaldı"
    bar_cls = "bar-green" if rem > 3600 else "bar-orange"
    return (lbl, pct, bar_cls)

def render_contract_cell(cn: str) -> str:
    label, pct, barcls = remaining_info(cn)
    bar = f"<div class='barwrap'><div class='barfill {barcls}' style='width:{pct}%'></div></div>"
    return f"<div class='cnwrap'><div class='cnhead'><span class='cnname'>{cn}</span><span class='cntimer'>{label}</span></div>{bar}</div>"

def yes_no_html(flag):
    if flag is None or (isinstance(flag, float) and np.isnan(flag)):
        return "-"
    return "<span class='yes'>✅</span>" if flag else "<span class='no'>✖</span>"

def format_aof(val):
    """AOF değerlerini 2 ondalık basamakla formatla"""
    if pd.isna(val): 
        return "-"
    return f"{val:.2f}"

def color_gap(val):
    if pd.isna(val): return "-"
    return f"<span class='gap-pos'>{val:.0f}</span>" if val >= 0 else f"<span class='gap-neg'>{val:.0f}</span>"

def color_ptf(val):
    """PTF değerlerini kırmızı arkaplan ile göster"""
    if pd.isna(val): 
        return "-"
    return f"<span style='background-color: #dc3545; color: white; padding: 2px 6px; border-radius: 3px; font-weight: bold;'>{val:.2f}</span>"

def format_min_price(val):
    """Min fiyatları yeşil arkaplan ile göster"""
    if pd.isna(val) or val == 0: 
        return "-"
    return f"<span style='background-color: #28a745; color: white; padding: 2px 6px; border-radius: 3px; font-weight: bold;'>{val:.2f}</span>"

def format_max_price(val):
    """Max fiyatları turuncu arkaplan ile göster"""
    if pd.isna(val) or val == 0: 
        return "-"
    return f"<span style='background-color: #fd7e14; color: white; padding: 2px 6px; border-radius: 3px; font-weight: bold;'>{val:.2f}</span>"

def send_telegram(text: str) -> bool:
    """Send telegram notification - only for admin users"""
    user_permissions = st.session_state.get('user_permissions', {})
    
    # TEST: Permission kontrolünü atla
    # if not user_permissions.get('telegram', False):
    #     return False
    
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS):
        return False
    ok = True
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            r = requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=10)
            ok = ok and (r.status_code == 200)
        except Exception:
            ok = False
    return ok

def age_str(dt):
    if dt is None or pd.isna(dt):
        return "—"
    sec = int((datetime.now() - dt).total_seconds())
    m, s = divmod(sec, 60); h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def map_unique(series_key: pd.Series, mapping_dict: dict):
    """Series.map ama mapping kaynağı dict; duplicate-index kaynaklı reindex hatası yok."""
    return series_key.map(mapping_dict)

def build_map_dict(df: pd.DataFrame, key: str, val: str) -> dict:
    """key-val mapping için: normalize et, duplicate key'leri son kayıtla düşür, dict döndür."""
    if df.empty or key not in df.columns or val not in df.columns:
        return {}
    tmp = df[[key, val]].dropna(subset=[key]).copy()
    tmp[key] = tmp[key].map(norm_cn)
    # son kaydı tut (CSV'de zaman sıralıysa zaten sondaki en güncel)
    tmp = tmp.drop_duplicates(subset=[key], keep="last")
    # dict
    return dict(zip(tmp[key].tolist(), tmp[val].tolist()))

def export_to_excel(dash_df: pd.DataFrame, last_trades_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Dashboard sheet
        if not dash_df.empty:
            export_cols = {
                'contractName': 'Kontrat',
                'kontrat_saat': 'Saat', 
                'PTF_show': 'PTF',
                'aof_show': 'AOF',
                'gap': 'GAP',
                'last_effective': 'Son Eşleşme',
                'last_gap': 'Son Eşleşme GAP',
                'flow_15m': 'Hacim Lot (15dk)'
            }
            
            df_final = dash_df[list(export_cols.keys())].rename(columns=export_cols)
            df_final.to_excel(writer, sheet_name='Dashboard', index=False)

        # Last trades sheet
        if not last_trades_df.empty and 'contractName' in last_trades_df.columns:
            trades = last_trades_df.copy()
            trades.columns = ['Kontrat', 'Zaman', 'Fiyat', 'Miktar']
            trades.to_excel(writer, sheet_name='Son İşlemler', index=False)

    return output.getvalue()

def handle_telegram_notifications(dash_df, threshold, interval):
    if not (st.session_state.telegram_running and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS):
        return
        
    now_ts = time.time()
    notified = st.session_state.get("last_notify", {})
    
    for _, r in dash_df.iterrows():
        cn = str(r["contractName"])
        chosen = None
        label = None
        
        if pd.notna(r["last_gap"]) and np.isfinite(r["last_gap"]):
            chosen = float(r["last_gap"])
            label = "SON EŞLEŞME - PTF GAP"
        elif pd.notna(r["gap"]) and np.isfinite(r["gap"]):
            chosen = float(r["gap"])
            label = "AOF - PTF GAP"
            
        if chosen is None:
            continue

        if abs(chosen) >= float(threshold):
            key = f"{cn}:{label}"
            if now_ts - notified.get(key, 0) >= interval:
                yon = "↑" if chosen > 0 else "↓"
                msg = f"{cn} kontratında {label} {yon}{int(round(chosen))} TL oldu!"
                if send_telegram(msg):
                    notified[key] = now_ts
    
    st.session_state["last_notify"] = notified

# ======================== Main Logic ========================
# Auto refresh removed - manual refresh only

# Initialize variables with defaults
aof_df = pd.DataFrame()
last_df = pd.DataFrame()
flow15_df = pd.DataFrame()
last_min_df = pd.DataFrame()
last_db_snap = None
last_csv_time = None

# Show data update status in sidebar
st.sidebar.markdown("### 📊 Veri Durumu")
now = datetime.now()

# Load board info with memory optimization
df_board = pd.DataFrame()  # Initialize with empty DataFrame
try:
    if os.path.exists(BOARDINFO_CSV):
        # Memory efficient: only read last 1000 lines instead of entire 118MB file
        try:
            # Read only last portion of CSV for performance
            with open(BOARDINFO_CSV, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
            # Keep header + last 1000 data lines
            if len(lines) > 1001:  # header + 1000 data lines
                header_line = lines[0]  # First line is header
                recent_data_lines = lines[-1000:]  # Last 1000 data lines
                recent_lines = [header_line] + recent_data_lines
            else:
                recent_lines = lines
                
            # Create DataFrame from recent lines
            from io import StringIO
            csv_content = ''.join(recent_lines)
            df_board = pd.read_csv(StringIO(csv_content), on_bad_lines="skip", encoding='utf-8')
            
            # Clean column names - remove leading/trailing whitespace
            df_board.columns = df_board.columns.str.strip()
            
            # Parse time column properly - try different formats
            if "time" in df_board.columns:
                try:
                    df_board["time"] = pd.to_datetime(df_board["time"], errors='coerce')
                    
                    # Get last CSV time from valid times only
                    valid_times = df_board["time"].dropna()
                    if not valid_times.empty:
                        last_csv_time = valid_times.max()
                    else:
                        last_csv_time = None
                        
                except Exception as e:
                    last_csv_time = None
                    
        except Exception as e:
            st.error(f"CSV okuma hatası: {e}")
            df_board = pd.DataFrame()
            last_csv_time = None
            
            # Add contract hour column FIRST - more robust parsing
            def extract_hour(contract_name):
                try:
                    if isinstance(contract_name, str) and len(contract_name) >= 10:
                        hour_str = contract_name[8:10]
                        return int(hour_str)
                    return None
                except:
                    return None
            
            df_board['kontrat_saat'] = df_board['contractName'].apply(extract_hour)
            
            # Convert numeric columns to float
            numeric_cols = ['mcp', 'averagePrice', 'lastPrice', 'total', 'volume', 'bestBuyPrice', 'bestSellPrice', 'minPrice', 'maxPrice']
            for col in numeric_cols:
                if col in df_board.columns:
                    df_board[col] = pd.to_numeric(df_board[col], errors='coerce')
                    
except Exception as e:
    st.error(f"Error loading board info: {str(e)}")
    st.exception(e)  # Show full traceback in the UI

# Load trades
try:
    with sqlite3.connect(DB_PATH) as con:
        # Load AOF data with better date handling
        aof_df = pd.read_sql_query("""
            SELECT 
                contractName,
                SUM(price*quantity)/NULLIF(SUM(quantity),0.0) AS aof,
                COUNT(*) as trade_count,
                MIN(time) as first_trade,
                MAX(time) as last_trade
            FROM trades
            WHERE date(time) = date('now','localtime')
            GROUP BY contractName
        """, con)
        
        # Load last trades with more info
        last_df = pd.read_sql_query("""
            SELECT 
                t.contractName, 
                t.price AS last_trade,
                t.quantity as last_quantity,
                t.time as trade_time
            FROM trades t
            JOIN (
                SELECT contractName, MAX(time) mt
                FROM trades
                GROUP BY contractName
            ) x ON x.contractName=t.contractName AND x.mt=t.time
        """, con)
        
        # Load flow data for last 15 minutes
        flow15_df = pd.read_sql_query("""
            SELECT 
                contractName, 
                SUM(quantity) AS flow_15m,
                COUNT(*) as trade_count_15m,
                MIN(price) as min_price_15m,
                MAX(price) as max_price_15m
            FROM trades
            WHERE datetime(time) >= datetime('now','localtime','-15 minutes')
            GROUP BY contractName
        """, con)
        
        # Load very recent trades
        last_min_df = pd.read_sql_query("""
            SELECT 
                contractName, 
                time,
                price, 
                quantity,
                aof_1h
            FROM trades
            WHERE datetime(time) >= datetime('now','localtime','-60 seconds')
            ORDER BY datetime(time) DESC
            LIMIT 200
        """, con)
        
        last_db_snap = datetime.now()
        
        # Show data freshness in sidebar
        if not last_min_df.empty:
            last_trade_time = pd.to_datetime(last_min_df['time'].iloc[0])
            trade_age = (now - last_trade_time).total_seconds()
            st.sidebar.info(f"Son işlem: {int(trade_age)} saniye önce")
        
except Exception as e:
    st.error(f"Error loading trade data: {str(e)}")
    st.exception(e)

# Initialize dashboard DataFrame with necessary columns
if df_board.empty:
    st.warning("Veri yok: df_board boş")
    dash = pd.DataFrame(columns=['contractName'])
else:
    # Get latest data for each contract FIRST
    latest_data = df_board.sort_values('time').groupby('contractName').last().reset_index()
    
    dash = latest_data.copy()
    
    # Convert main metrics
    dash['PTF_show'] = pd.to_numeric(dash['mcp'], errors='coerce')
    
    # Use real trade data for AOF and last trade instead of board info
    if not aof_df.empty:
        aof_dict = aof_df.set_index('contractName')['aof'].to_dict()
        dash['aof_show'] = dash['contractName'].map(aof_dict).fillna(0)
    else:
        dash['aof_show'] = pd.to_numeric(dash['averagePrice'], errors='coerce')  # Fallback to board data
    
    if not last_df.empty:
        last_dict = last_df.set_index('contractName')['last_trade'].to_dict()
        dash['last_effective'] = dash['contractName'].map(last_dict).fillna(0)
    else:
        dash['last_effective'] = pd.to_numeric(dash['lastPrice'], errors='coerce')  # Fallback to board data
    
    # Calculate gaps with proper float conversion
    dash['gap'] = (dash['aof_show'] - dash['PTF_show']).fillna(0)
    dash['last_gap'] = (dash['last_effective'] - dash['PTF_show']).fillna(0)

    # Calculate comparison indicators safely
    dash['aof_gt'] = (pd.to_numeric(dash['aof_show'], errors='coerce') > pd.to_numeric(dash['PTF_show'], errors='coerce')).fillna(False)
    dash['last_gt'] = (pd.to_numeric(dash['last_effective'], errors='coerce') > pd.to_numeric(dash['PTF_show'], errors='coerce')).fillna(False)

# Helper function for time filtering
def time_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or 'kontrat_saat' not in df.columns:
        return df
    
    # Remove rows with null kontrat_saat and convert to numeric
    df_filtered = df.dropna(subset=['kontrat_saat']).copy()
    df_filtered['kontrat_saat'] = pd.to_numeric(df_filtered['kontrat_saat'], errors='coerce')
    df_filtered = df_filtered.dropna(subset=['kontrat_saat'])  # Remove any conversion failures
    
    if zaman == "Tümü":
        return df_filtered
    elif zaman.startswith("Sabah"):
        return df_filtered[(df_filtered["kontrat_saat"] >= 1) & (df_filtered["kontrat_saat"] <= 8)]
    elif zaman.startswith("Öğle"):
        return df_filtered[(df_filtered["kontrat_saat"] >= 9) & (df_filtered["kontrat_saat"] <= 16)]
    elif zaman.startswith("Akşam"):
        return df_filtered[(df_filtered["kontrat_saat"] >= 17) & (df_filtered["kontrat_saat"] <= 24)]
    elif zaman == "Özel" and cs is not None and ce is not None:
        a, b = int(cs), int(ce)
        if a <= b:
            return df_filtered[(df_filtered["kontrat_saat"] >= a) & (df_filtered["kontrat_saat"] <= b)]
        else:
            return df_filtered[(df_filtered["kontrat_saat"] >= a) | (df_filtered["kontrat_saat"] <= b)]
    return df_filtered

# Apply time filters - but only if we have data
dash = time_filter(dash)

if not show_closed:
    now = datetime.now()
    dash = dash[dash["contractName"].apply(lambda cn: (contract_cutoff(cn) is not None) and (now <= contract_cutoff(cn)))]

# Apply visual formatting
dash["cn_html"] = dash["contractName"].astype(str).apply(render_contract_cell)
dash["gap_html"] = dash["gap"].apply(color_gap)
dash["last_gap_html"] = dash["last_gap"].apply(color_gap)
dash["aof_gt_icon"] = dash["aof_gt"].apply(yes_no_html)
dash["last_gt_icon"] = dash["last_gt"].apply(yes_no_html)
dash["ptf_html"] = dash["PTF_show"].apply(color_ptf)
dash["aof_html"] = dash["aof_show"].apply(format_aof)

# Sort and remove duplicates
dash = dash.sort_values(["contractName"]).copy()
dash = dash.loc[:, ~dash.columns.duplicated()]

# Add min/max price formatting if columns exist
if "minPrice" in dash.columns and "maxPrice" in dash.columns:
    # Calculate min/max from reasonable price columns only
    reasonable_price_cols = ['mcp', 'averagePrice', 'lastPrice']
    
    for contract in dash['contractName'].unique():
        contract_board_data = df_board[df_board['contractName'] == contract]
        
        if not contract_board_data.empty:
            # Collect all valid price values for this contract from reasonable columns
            all_prices = []
            for col in reasonable_price_cols:
                if col in contract_board_data.columns:
                    prices = pd.to_numeric(contract_board_data[col], errors='coerce').dropna()
                    # Filter out unreasonable values (should be between 100 and 5000 TL for electricity)
                    reasonable_prices = prices[(prices > 100) & (prices < 5000)]
                    all_prices.extend(reasonable_prices.tolist())
            
            # Calculate min/max from reasonable prices
            if all_prices:
                min_price = min(all_prices)
                max_price = max(all_prices)
                
                # Update the dash DataFrame
                dash.loc[dash['contractName'] == contract, 'minPrice'] = min_price
                dash.loc[dash['contractName'] == contract, 'maxPrice'] = max_price
    
    dash["min_price_html"] = dash["minPrice"].apply(format_min_price)
    dash["max_price_html"] = dash["maxPrice"].apply(format_max_price)

# Display the main table
st.markdown(f"### 📊 Kontrat Tablosu <span style='color:#00ff00; font-size:0.8em;'>🔴 CANLI: {datetime.now().strftime('%H:%M:%S')}</span>", unsafe_allow_html=True)


if dash.empty:
    st.warning("⚠️ Gösterilecek veri yok!")
    st.info("Lütfen bekleyin veya filtreleri kontrol edin")
    st.info(f"Board data rows: {len(df_board)}")
else:
    # Prepare display columns
    display_cols = ["cn_html", "ptf_html", "aof_html"]
    
    if "gap_html" in dash.columns:
        display_cols.extend(["aof_gt_icon", "gap_html"])
    
    if "last_effective" in dash.columns:
        display_cols.extend(["last_effective", "last_gap_html", "last_gt_icon"])
    
    # Add min/max price columns if available (remove volume)
    if "min_price_html" in dash.columns:
        display_cols.append("min_price_html")
        
    if "max_price_html" in dash.columns:
        display_cols.append("max_price_html")
    
    # Filter existing columns only
    available_cols = [col for col in display_cols if col in dash.columns]
    
    if available_cols:
        df_show = dash[available_cols].copy()
        
        # Create appropriate column names
        col_names = ["Kontrat", "PTF", "AOF"]
        if "gap_html" in available_cols:
            col_names.extend(["AOF > PTF", "GAP"])
        if "last_effective" in available_cols:
            col_names.extend(["Son Eşleşme", "Son Eşleşme GAP", "Son Eşleşme > PTF"])
        if "min_price_html" in available_cols:
            col_names.append("Min Fiyat")
        if "max_price_html" in available_cols:
            col_names.append("Max Fiyat")
        
        df_show.columns = col_names[:len(df_show.columns)]
        
        # Display the table
        st.markdown(df_show.to_html(escape=False, index=False), unsafe_allow_html=True)
        
        # Show data count info
        st.caption(f"Toplam {len(df_show)} kontrat gösteriliyor")
    else:
        st.error("Görüntülenecek sütun bulunamadı")

# Display recent trades
st.markdown("### Son 1 Dakikalık Eşleşmeler")
if 'contractName' not in (last_min_df.columns if not last_min_df.empty else []):
    st.write("Veri yok.")
else:
    lmd = last_min_df.copy()
    lmd = lmd.loc[:, ~lmd.columns.duplicated()]
    lmd["contractName"] = lmd["contractName"].map(norm_cn)
    if "time" in lmd.columns:
        lmd["time"] = pd.to_datetime(lmd["time"], errors="coerce").dt.strftime("%H:%M:%S")
    lmd = lmd.rename(columns={"contractName":"Kontrat", "time":"Zaman", "price":"Fiyat", "quantity":"Miktar"})
    st.dataframe(lmd, hide_index=True, use_container_width=True)

# Show visualizations if enabled
if show_charts and not dash.empty:
    st.markdown("### 📊 Görselleştirmeler")
    
    try:
        # Prepare data for charts
        chart_data = dash.copy()
        
        # Ensure we have numeric data for charts
        if 'kontrat_saat' in chart_data.columns and 'PTF_show' in chart_data.columns and 'aof_show' in chart_data.columns:
            # Remove rows with missing values
            chart_data = chart_data.dropna(subset=['kontrat_saat', 'PTF_show', 'aof_show'])
            
            if not chart_data.empty:
                # Hourly PTF vs AOF comparison
                chart_melted = pd.melt(
                    chart_data, 
                    id_vars=['kontrat_saat'], 
                    value_vars=['PTF_show', 'aof_show'],
                    var_name='Tip', 
                    value_name='Fiyat'
                )
                chart_melted['Tip'] = chart_melted['Tip'].map({'PTF_show': 'PTF', 'aof_show': 'AOF'})
                
                fig = px.line(
                    chart_melted, 
                    x="kontrat_saat", 
                    y="Fiyat", 
                    color="Tip",
                    title="Saatlik PTF vs AOF Karşılaştırması",
                    labels={'kontrat_saat': 'Saat', 'Fiyat': 'Fiyat (TL)'}
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Grafik için yeterli veri yok (PTF/AOF)")
        
        # Volume analysis if available
        if 'volume' in chart_data.columns and 'kontrat_saat' in chart_data.columns:
            vol_data = chart_data.dropna(subset=['kontrat_saat', 'volume'])
            if not vol_data.empty and len(vol_data) > 1:
                vol_fig = px.scatter(
                    vol_data, 
                    x="kontrat_saat", 
                    y="volume",
                    title="Saatlik İşlem Hacmi Dağılımı",
                    labels={'kontrat_saat': 'Saat', 'volume': 'Hacim'}
                )
                st.plotly_chart(vol_fig, use_container_width=True)
            else:
                st.info("Hacim grafiği için yeterli veri yok")
                
    except Exception as e:
        st.error(f"Grafik oluşturma hatası: {str(e)}")

elif show_charts:
    st.warning("Grafik göstermek için veri gerekli")

if show_analytics and not dash.empty:
    st.markdown("### 📈 Analitikler")
    
    try:
        # Calculate analytics with error handling
        analytics_data = dash.copy()
        
        # Ensure we have required columns
        required_cols = ['PTF_show', 'aof_show', 'gap', 'volume']
        available_cols = [col for col in required_cols if col in analytics_data.columns]
        
        if len(available_cols) >= 2:  # At least PTF and AOF
            # Calculate safe analytics
            total_volume = analytics_data['volume'].sum() if 'volume' in analytics_data.columns else 0
            avg_ptf = analytics_data['PTF_show'].mean() if not analytics_data['PTF_show'].isna().all() else 0
            
            if 'gap' in analytics_data.columns:
                max_gap = analytics_data['gap'].max() if not analytics_data['gap'].isna().all() else 0
                min_gap = analytics_data['gap'].min() if not analytics_data['gap'].isna().all() else 0
            else:
                max_gap = min_gap = 0
            
            # Display metrics with safe formatting
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Toplam İşlem Hacmi", f"{total_volume:,.0f}" if total_volume else "N/A")
            c2.metric("Ortalama PTF", f"{avg_ptf:,.1f} ₺" if avg_ptf else "N/A")
            c3.metric("En Yüksek GAP", f"{max_gap:,.1f} ₺" if max_gap else "N/A")
            c4.metric("En Düşük GAP", f"{min_gap:,.1f} ₺" if min_gap else "N/A")
            
            # Additional analytics if we have enough data
            if len(analytics_data) > 1 and 'kontrat_saat' in analytics_data.columns:
                # Hourly volume analysis
                if 'volume' in analytics_data.columns:
                    hourly_vol = analytics_data.groupby('kontrat_saat')['volume'].mean().reset_index()
                    if not hourly_vol.empty:
                        vol_chart = px.bar(
                            hourly_vol, 
                            x='kontrat_saat', 
                            y='volume',
                            title="Saatlik Ortalama Hacim",
                            labels={'kontrat_saat': 'Saat', 'volume': 'Ortalama Hacim'}
                        )
                        st.plotly_chart(vol_chart, use_container_width=True)
        else:
            st.warning("Analitik için yeterli veri yok")
            
    except Exception as e:
        st.error(f"Analitik hesaplama hatası: {str(e)}")

elif show_analytics:
    st.warning("Analitik göstermek için veri gerekli")

# Export button with unique key
if st.sidebar.button("Excel'e Aktar", use_container_width=True, key="export_btn_main"):
    try:
        excel_bytes = export_to_excel(dash, last_min_df)
        st.sidebar.download_button(
            label="📥 Excel Dosyasını İndir",
            data=excel_bytes,
            file_name=f"gip_dashboard_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="download_btn_main"
        )
        st.sidebar.success("✅ Veriler Excel'e aktarıldı!")
    except Exception as e:
        st.sidebar.error(f"Excel oluşturma hatası: {str(e)}")

# ==================== ALARM SİSTEMİ ====================
def check_alarms_for_telegram(current_data, alarm_settings):
    """İlk çalıştırmada tüm alarmları kontrol et, sonrasında sadece yenileri"""
    alarms = []
    
    if not alarm_settings['enabled'] or current_data.empty:
        return alarms
    
    open_contracts_count = 0
    processed_contracts = 0
    
    for _, current_row in current_data.iterrows():
        contract_name = current_row['contractName']
        processed_contracts += 1
        
        # Kontrat saatini çıkar (örn: PTF140814 -> 14)
        try:
            if isinstance(contract_name, str) and len(contract_name) >= 10:
                contract_hour = int(contract_name[8:10])
            else:
                continue
        except:
            continue
        
        # Sadece açık kontratlar için alarm ver (kapalı kontratlar için alarm yok)
        cutoff_time = contract_cutoff(contract_name)
        if cutoff_time is None:
            continue  # Geçersiz kontrat formatı
        
        current_time = datetime.now()
        if current_time > cutoff_time:
            continue  # Kontrat kapanmış, alarm verme
        
        open_contracts_count += 1
        
        # GAP kontrolü (Son Eşleşme - PTF; mutlak eşik, mesajda AOF/PTF göster)
        if 'last_gap' in current_row and 'mcp' in current_row and 'averagePrice' in current_row:
            last_gap_signed = pd.to_numeric(current_row['last_gap'], errors='coerce')
            ptf_price = pd.to_numeric(current_row['mcp'], errors='coerce')
            aof_price = pd.to_numeric(current_row['averagePrice'], errors='coerce')
            
            if pd.notna(last_gap_signed) and pd.notna(ptf_price):
                gap = abs(last_gap_signed)
                gap_threshold = alarm_settings.get('gap_threshold', 5.0)
                
                # GAP kontrolü (sadece eşik aşımında alarm üret)
                if gap >= gap_threshold:
                    direction = "🔺" if last_gap_signed > 0 else "🔻"
                    # Alarm yapısına ham değerleri ekle (telegram formatı için)
                    alarms.append({
                        'type': 'gap_alert',
                        'contract': contract_name,
                        'message': f"{direction} {contract_name} (AÇIK-S{contract_hour})",
                        'severity': 'high' if gap >= gap_threshold * 2 else 'medium',
                        'gap_value': float(gap),              # mutlak GAP
                        'gap_signed': float(last_gap_signed), # işaretli GAP
                        'last_effective': float(current_row.get('last_effective', 0)) if pd.notna(current_row.get('last_effective', 0)) else None,
                        'ptf': float(ptf_price) if pd.notna(ptf_price) else None,
                        'aof': float(aof_price) if pd.notna(aof_price) else None,
                        'contract_hour': int(contract_hour) if pd.notna(contract_hour) else None
                    })
    
    return alarms

# Alarm sistemi kontrolü
if alarm_enabled and not df_board.empty:
    # Session state'te önceki veriyi sakla
    if 'previous_board_data' not in st.session_state:
        st.session_state.previous_board_data = df_board.copy()
    
    # Alarm ayarları - kullanıcı tanımlı ve sabit değerler
    alarm_settings = {
        'enabled': alarm_enabled,
        'price_change_threshold': 10,  # %10 fiyat değişimi
        'volume_increase_threshold': 100,  # %100 hacim artışı
        'spread_threshold': 50,  # 50 TL spread
        'gap_threshold': st.session_state.get('telegram_gap_threshold', 5.0)  # Kullanıcı tanımlı GAP eşiği
    }
    
    # Alarm verisi olarak dash DataFrame'ini kullan (latest_data yerine)
    # dash zaten filtrelenmiş ve işlenmiş veri, last_gap sütunu da var
    current_alarm_data = dash.copy()  # İşlenmiş tablo verisi
    
    # Önceki veriyi de aynı şekilde işle
    if 'previous_board_data' in st.session_state:
        previous_board = st.session_state.previous_board_data
        # Önceki veriyi de aynı şekilde işle
        prev_latest = previous_board.sort_values('time').groupby('contractName').last().reset_index()
        prev_dash = prev_latest.copy()
        
        # Aynı hesaplamaları yap
        prev_dash['PTF_show'] = pd.to_numeric(prev_dash['mcp'], errors='coerce')
        prev_dash['last_effective'] = pd.to_numeric(prev_dash['lastPrice'], errors='coerce')
        prev_dash['last_gap'] = (prev_dash['last_effective'] - prev_dash['PTF_show']).fillna(0)
        
        previous_alarm_data = prev_dash
    else:
        previous_alarm_data = current_alarm_data.copy()
    
    # DEBUG: Kontrol edilen veriler (sadece dev ortamında)
    # st.info(f"🔍 {len(current_alarm_data)} kontrat kontrol ediliyor")
    # if not current_alarm_data.empty:
    #     st.write("📊 İlk 3 kontrat:", current_alarm_data[['contractName', 'last_effective', 'PTF_show', 'last_gap']].head(3))
    
    # Alarm kontrolü - sadece GAP alarmları için özel fonksiyon kullan
    alarms = check_alarms_for_telegram(current_alarm_data, alarm_settings)
    
    # DEBUG: Üretilen alarmlar (sadece dev ortamında)
    # st.info(f"⚠️ {len(alarms)} alarm üretildi")
    # if alarms:
    #     for alarm in alarms[:3]:  # İlk 3 alarmı göster
    #         st.write(f"🚨 {alarm['contract']}: GAP {alarm.get('gap_value', 0):.2f} TL")
    
    # Telegram bildirimi sistem - kontrat bazlı cooldown
    if alarms and (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS):
        current_time = time.time()
        alert_interval = st.session_state.get('telegram_alert_interval', 30)  # dakika
        alert_interval_seconds = alert_interval * 60  # saniyeye çevir
        
        # İlk çalıştırma kontrolü
        is_first_run = not st.session_state.get('first_run_alarms_sent', False)
        
        # Session state'te her kontrat için son bildirim zamanını sakla
        if 'last_telegram_per_contract' not in st.session_state:
            st.session_state.last_telegram_per_contract = {}
        
        alerts_to_send = []
        
        for alarm in alarms:
            if alarm['type'] == 'gap_alert':  # Sadece GAP alarmları
                contract = alarm['contract']
                last_sent_time = st.session_state.last_telegram_per_contract.get(contract, 0)
                time_since_last = current_time - last_sent_time
                
                # İlk çalıştırmada veya cooldown süresi geçmişse gönder
                should_send = is_first_run or (time_since_last >= alert_interval_seconds)
                
                if should_send:
                    alerts_to_send.append(alarm)
                    st.session_state.last_telegram_per_contract[contract] = current_time
        
        # İlk çalıştırma bayrağını set et
        if is_first_run and alerts_to_send:
            st.session_state.first_run_alarms_sent = True
            st.success(f"🚀 İlk çalıştırma: {len(alerts_to_send)} alarm bildirimi gönderiliyor...")
        elif alerts_to_send:
            st.info(f"⏰ Cooldown süresi geçti: {len(alerts_to_send)} alarm bildirimi gönderiliyor...")
        
        # Telegram bildirimlerini gönder
        for alarm in alerts_to_send:
            contract = alarm['contract']
            gap_abs = alarm.get('gap_value', 0.0)
            gap_signed = alarm.get('gap_signed', 0.0)
            last_effective = alarm.get('last_effective')
            ptf = alarm.get('ptf')
            aof = alarm.get('aof')

            # Metni istenen formatta oluştur
            signed_symbol = '+' if gap_signed is not None and gap_signed >= 0 else ''
            last_line = f"Son Eşleşme GAP: {signed_symbol}{gap_signed:.2f} TL" if gap_signed is not None else "Son Eşleşme GAP: N/A"
            aof_line = f"AOF: {aof:.2f} TL" if aof is not None else "AOF: N/A"
            ptf_line = f"PTF: {ptf:.2f} TL" if ptf is not None else "PTF: N/A"

            telegram_text = (
                "🚨 GİP GAP ALARMI 🚨\n\n"
                f"📊 Kontrat: {contract}\n"
                f"{last_line}\n"
                f"{aof_line}\n"
                f"{ptf_line}\n\n"
                f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
                f"🎯 Eşik: {alarm_settings.get('gap_threshold', 5.0)} TL\n"
                f"⏱️ Sonraki bildirim: {alert_interval} dk sonra"
            )
            
            if send_telegram(telegram_text):
                st.success(f"📱 Telegram bildirimi gönderildi: {contract} - GAP: {gap_signed:.2f}")
            else:
                st.error(f"❌ Telegram bildirimi gönderilemedi: {contract} - Token/Chat ID kontrol et")
    
    # Sadece görsel alarm gösterimi için geleneksel alarm sistemi (debounce ile)
    current_time = time.time()
    
    # Alarm debounce sistemi - sadece görsel gösterim için
    new_alarms = []
    if 'shown_alarms' not in st.session_state:
        st.session_state.shown_alarms = {}
    
    for alarm in alarms:
        alarm_key = f"{alarm['contract']}_{alarm['type']}"
        last_shown = st.session_state.shown_alarms.get(alarm_key, 0)
        
        # Görsel alarmlar için 5 dakikalık cooldown (Telegram'dan bağımsız)
        visual_cooldown = 5 * 60  # 5 dakika
        if current_time - last_shown >= visual_cooldown:
            new_alarms.append(alarm)
            st.session_state.shown_alarms[alarm_key] = current_time
    
    # Sadece yeni alarmları görsel olarak göster
    if new_alarms:
        st.markdown("### 🚨 AKTİF ALARMLAR")
        
        for alarm in new_alarms:
            if alarm['severity'] == 'high':
                st.error(alarm['message'])
            else:
                st.warning(alarm['message'])
        
        # Alarm geçmişini session state'te sakla
        if 'alarm_history' not in st.session_state:
            st.session_state.alarm_history = []
        
        # Sadece yeni alarmları geçmişe ekle
        for alarm in new_alarms:
            alarm['timestamp'] = datetime.now()
            st.session_state.alarm_history.append(alarm)
            
        # Son 20 alarmı sakla
        st.session_state.alarm_history = st.session_state.alarm_history[-20:]
    
    # Alarm geçmişini göster - sadece alarm aktifse
    if 'alarm_history' in st.session_state and st.session_state.alarm_history:
        with st.expander("📋 Alarm Geçmişi (Son 10)"):
            for alarm in reversed(st.session_state.alarm_history[-10:]):
                st.text(f"{alarm['timestamp'].strftime('%H:%M:%S')} - {alarm['message']}")


# Alarm kapalıysa tüm alarm verilerini temizle ve hiçbir şey gösterme
else:
    # Alarm verilerini temizle
    for key in ['alarm_history', 'previous_board_data', 'shown_alarms', 'last_telegram_per_contract', 'first_run_alarms_sent']:
        if key in st.session_state:
            del st.session_state[key]
    # Hiçbir alarm bölümü gösterme - tamamen temiz

# Checkbox durumlarını session state'te sakla
if 'checkbox_states' not in st.session_state:
    st.session_state.checkbox_states = {
        'show_charts': False,
        'show_analytics': False, 
        'alarm_enabled': False
    }

# Checkbox durumlarını güncelle
st.session_state.checkbox_states['show_charts'] = show_charts
st.session_state.checkbox_states['show_analytics'] = show_analytics
st.session_state.checkbox_states['alarm_enabled'] = alarm_enabled
