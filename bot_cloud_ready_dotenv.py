import json
import csv
import os
import logging
import asyncio

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()
from pathlib import Path
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple

import pandas as pd
import yfinance as yf

from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# =========================================================
# CONFIG
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
AUTHORIZED_CHAT_ID_RAW = os.getenv("AUTHORIZED_CHAT_ID", "").strip()

if AUTHORIZED_CHAT_ID_RAW:
    try:
        AUTHORIZED_CHAT_ID = int(AUTHORIZED_CHAT_ID_RAW)
    except ValueError as exc:
        raise ValueError("AUTHORIZED_CHAT_ID must be a valid integer.") from exc
else:
    AUTHORIZED_CHAT_ID = 0

IST = ZoneInfo("Asia/Kolkata")

DATA_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

ALERT_STATE_FILE = str(DATA_DIR / "alert_state.json")
CONFIG_FILE = str(DATA_DIR / "bot_config.json")
STATUS_FILE = str(DATA_DIR / "bot_status.json")
TRADE_LOG_FILE = str(DATA_DIR / "trade_log.csv")

DEFAULT_ALERT_ONLY_MODE = True
DEFAULT_ALERT_THRESHOLD = 75
DEFAULT_ALERT_COOLDOWN_MINUTES = 45

DEFAULT_INTERVAL = "15m"
DEFAULT_PERIOD = "5d"

HIGHER_TF_INTERVAL = "60m"
HIGHER_TF_PERIOD = "10d"

MIN_REQUIRED_BARS = 100
STALE_DATA_MAX_DELAY_MINUTES = 35
TRADE_MAX_AGE_DAYS = 2

SYMBOLS = {
    "NIFTY 50": "^NSEI",
    "BANK NIFTY": "^NSEBANK",
    "SENSEX": "^BSESN",
}

ALERT_ONLY_MODE = DEFAULT_ALERT_ONLY_MODE
ALERT_THRESHOLD = DEFAULT_ALERT_THRESHOLD
ALERT_COOLDOWN_MINUTES = DEFAULT_ALERT_COOLDOWN_MINUTES

LAST_ALERTS: Dict[str, datetime] = {}
LAST_SCAN_TIME: Optional[datetime] = None
LAST_ALERT_TIME: Optional[datetime] = None
ALERTS_SENT_TODAY = 0
ALERTS_SENT_DATE: Optional[str] = None

SCAN_LOCK = asyncio.Lock()
LAST_SCAN_RESULTS: Dict[str, dict] = {}
LAST_SCAN_SUMMARY_TEXT = "No scan yet"


# =========================================================
# AUTH
# =========================================================
def is_authorized(update: Update) -> bool:
    if update.effective_chat is None:
        return False
    return update.effective_chat.id == AUTHORIZED_CHAT_ID


async def require_authorized(update: Update) -> bool:
    if not is_authorized(update):
        if update.effective_message is not None:
            await update.effective_message.reply_text("Unauthorized user")
        return False
    return True


# =========================================================
# PERSISTENCE
# =========================================================
def load_config():
    global ALERT_ONLY_MODE, ALERT_THRESHOLD, ALERT_COOLDOWN_MINUTES

    if not os.path.exists(CONFIG_FILE):
        save_config()
        return

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)

        ALERT_ONLY_MODE = bool(config.get("alert_only_mode", DEFAULT_ALERT_ONLY_MODE))
        ALERT_THRESHOLD = int(config.get("alert_threshold", DEFAULT_ALERT_THRESHOLD))
        ALERT_COOLDOWN_MINUTES = int(
            config.get("alert_cooldown_minutes", DEFAULT_ALERT_COOLDOWN_MINUTES)
        )
    except Exception as e:
        logger.warning("Failed to load config: %s", e)
        ALERT_ONLY_MODE = DEFAULT_ALERT_ONLY_MODE
        ALERT_THRESHOLD = DEFAULT_ALERT_THRESHOLD
        ALERT_COOLDOWN_MINUTES = DEFAULT_ALERT_COOLDOWN_MINUTES


def save_config():
    config = {
        "alert_only_mode": ALERT_ONLY_MODE,
        "alert_threshold": ALERT_THRESHOLD,
        "alert_cooldown_minutes": ALERT_COOLDOWN_MINUTES,
    }

    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save config: %s", e)


def load_alert_state():
    global LAST_ALERTS

    if not os.path.exists(ALERT_STATE_FILE):
        LAST_ALERTS = {}
        return

    try:
        with open(ALERT_STATE_FILE, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        loaded = {}
        for key, value in raw_data.items():
            loaded[key] = datetime.fromisoformat(value)

        LAST_ALERTS = loaded
    except Exception as e:
        logger.warning("Failed to load alert state: %s", e)
        LAST_ALERTS = {}


def save_alert_state():
    try:
        serializable = {}
        for key, value in LAST_ALERTS.items():
            serializable[key] = value.isoformat()

        with open(ALERT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save alert state: %s", e)


def load_status():
    global LAST_SCAN_TIME, LAST_ALERT_TIME, ALERTS_SENT_TODAY, ALERTS_SENT_DATE

    if not os.path.exists(STATUS_FILE):
        save_status()
        return

    try:
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        LAST_SCAN_TIME = (
            datetime.fromisoformat(data["last_scan_time"])
            if data.get("last_scan_time")
            else None
        )
        LAST_ALERT_TIME = (
            datetime.fromisoformat(data["last_alert_time"])
            if data.get("last_alert_time")
            else None
        )
        ALERTS_SENT_TODAY = int(data.get("alerts_sent_today", 0))
        ALERTS_SENT_DATE = data.get("alerts_sent_date")
    except Exception as e:
        logger.warning("Failed to load status: %s", e)
        LAST_SCAN_TIME = None
        LAST_ALERT_TIME = None
        ALERTS_SENT_TODAY = 0
        ALERTS_SENT_DATE = None


def save_status():
    try:
        data = {
            "last_scan_time": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None,
            "last_alert_time": LAST_ALERT_TIME.isoformat() if LAST_ALERT_TIME else None,
            "alerts_sent_today": ALERTS_SENT_TODAY,
            "alerts_sent_date": ALERTS_SENT_DATE,
        }

        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save status: %s", e)


def reset_daily_alert_counter_if_needed():
    global ALERTS_SENT_TODAY, ALERTS_SENT_DATE

    today_str = datetime.now(IST).date().isoformat()
    if ALERTS_SENT_DATE != today_str:
        ALERTS_SENT_TODAY = 0
        ALERTS_SENT_DATE = today_str
        save_status()


def cleanup_old_alerts():
    now = datetime.now(IST)
    keys_to_delete = []

    for key, timestamp in LAST_ALERTS.items():
        if now - timestamp > timedelta(hours=24):
            keys_to_delete.append(key)

    for key in keys_to_delete:
        del LAST_ALERTS[key]

    if keys_to_delete:
        save_alert_state()


# =========================================================
# TRADE LOGGER
# =========================================================
TRADE_LOG_COLUMNS = [
    "trade_id",
    "signal_time",
    "symbol",
    "yahoo_symbol",
    "signal",
    "entry_mode",
    "trigger_price",
    "entry_price",
    "entry_time",
    "stop_loss",
    "target_price",
    "trailing_sl",
    "price_at_signal",
    "confidence",
    "regime",
    "trend",
    "htf_trend",
    "adx",
    "vwap",
    "volume_breakout",
    "status",
    "outcome",
    "exit_price",
    "exit_time",
    "rr",
    "notes",
]


def init_trade_log():
    if not Path(TRADE_LOG_FILE).exists():
        with open(TRADE_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(TRADE_LOG_COLUMNS)


def load_trade_log_df() -> pd.DataFrame:
    init_trade_log()
    try:
        df = pd.read_csv(TRADE_LOG_FILE)
        if df.empty:
            return pd.DataFrame(columns=TRADE_LOG_COLUMNS)
        for col in TRADE_LOG_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df
    except Exception as e:
        logger.warning("Failed to load trade log: %s", e)
        return pd.DataFrame(columns=TRADE_LOG_COLUMNS)


def save_trade_log_df(df: pd.DataFrame):
    try:
        df.to_csv(TRADE_LOG_FILE, index=False)
    except Exception as e:
        logger.warning("Failed to save trade log: %s", e)


def log_trade(result: dict):
    try:
        df = load_trade_log_df()

        trigger_price = safe_float(result.get("entry_trigger_price", result.get("price", 0)))
        entry_mode = result.get("entry", "No Trade")
        signal_time = datetime.now(IST)
        trade_id = f"{result.get('name','UNKNOWN')}-{signal_time.strftime('%Y%m%d%H%M%S')}"

        new_row = {
            "trade_id": trade_id,
            "signal_time": signal_time.isoformat(),
            "symbol": result.get("name"),
            "yahoo_symbol": SYMBOLS.get(result.get("name"), ""),
            "signal": result.get("signal"),
            "entry_mode": entry_mode,
            "trigger_price": trigger_price,
            "entry_price": "",
            "entry_time": "",
            "stop_loss": result.get("stop_loss", 0),
            "target_price": result.get("target_price", 0),
            "trailing_sl": result.get("trailing_sl", 0),
            "price_at_signal": result.get("price", 0),
            "confidence": result.get("confidence", 0),
            "regime": result.get("regime", "UNKNOWN"),
            "trend": result.get("trend", "Neutral"),
            "htf_trend": result.get("htf_trend", "Neutral"),
            "adx": result.get("adx", 0),
            "vwap": result.get("vwap", 0),
            "volume_breakout": result.get("volume_breakout", False),
            "status": "PENDING",
            "outcome": "",
            "exit_price": "",
            "exit_time": "",
            "rr": "",
            "notes": "",
        }

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_trade_log_df(df)

    except Exception as e:
        logger.warning("Trade log failed: %s", e)


# =========================================================
# HELPERS
# =========================================================
def safe_float(value, default=0.0) -> float:
    try:
        if pd.isna(value):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def fmt_dt(dt_obj):
    if dt_obj is None:
        return "Never"
    return dt_obj.astimezone(IST).strftime("%Y-%m-%d %I:%M:%S %p IST")


def extract_series(data: pd.DataFrame, column_name: str) -> pd.Series:
    col = data[column_name]

    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]

    col = pd.Series(col).copy()
    col = pd.to_numeric(col, errors="coerce")
    col = col.dropna()
    return col


def normalize_ohlc_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame()

    df = data.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    required = ["Open", "High", "Low", "Close"]
    for col in required:
        if col not in df.columns:
            return pd.DataFrame()

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    return df


def is_market_weekday() -> bool:
    return datetime.now(IST).weekday() < 5


def is_market_hours_now() -> bool:
    now = datetime.now(IST).time()
    return dt_time(9, 15) <= now <= dt_time(15, 30)


def is_market_open_day() -> bool:
    return is_market_weekday()


def is_active_market_session() -> bool:
    return is_market_hours_now()


def get_data_age_minutes(data: pd.DataFrame) -> float:
    try:
        if data is None or data.empty:
            return 999999.0

        last_idx = data.index[-1]

        if hasattr(last_idx, "to_pydatetime"):
            last_ts = last_idx.to_pydatetime()
        else:
            last_ts = pd.Timestamp(last_idx).to_pydatetime()

        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=IST)
        else:
            last_ts = last_ts.astimezone(IST)

        now = datetime.now(IST)
        diff = now - last_ts
        return max(diff.total_seconds() / 60.0, 0.0)
    except Exception:
        return 999999.0


def is_data_stale(data: pd.DataFrame, max_delay_minutes: int = STALE_DATA_MAX_DELAY_MINUTES) -> bool:
    if not is_market_weekday():
        return False
    if not is_market_hours_now():
        return False
    return get_data_age_minutes(data) > max_delay_minutes


def build_compact_result_line(result: dict) -> str:
    return (
        f"{result.get('name', 'UNKNOWN')} | "
        f"Signal: {result.get('signal', 'NONE')} | "
        f"Confidence: {result.get('confidence', 0)}% | "
        f"Strength: {result.get('strength', '❌ WEAK')} | "
        f"Entry: {result.get('entry', 'No Trade')}"
    )


def build_scan_summary(results: list) -> str:
    lines = ["📋 LAST SCAN SUMMARY", ""]
    for result in results:
        lines.append(build_compact_result_line(result))
    return "\n".join(lines)


def cache_scan_results(results: list):
    global LAST_SCAN_RESULTS, LAST_SCAN_SUMMARY_TEXT
    LAST_SCAN_RESULTS = {result["name"]: result for result in results}
    LAST_SCAN_SUMMARY_TEXT = build_scan_summary(results)


# =========================================================
# AGGRESSIVE SMART MODE
# =========================================================
def aggressive_signal_boost(result: dict) -> dict:
    try:
        signal = result.get("signal", "NONE")
        trend = result.get("trend", "Neutral")
        htf_trend = result.get("htf_trend", "Neutral")
        adx = safe_float(result.get("adx", 0), 0)
        price = safe_float(result.get("price", 0), 0)
        vwap = safe_float(result.get("vwap", 0), 0)
        confidence = int(safe_float(result.get("confidence", 0), 0))
        regime = str(result.get("regime", "UNKNOWN"))

        if regime not in ["TREND DAY", "BREAKOUT DAY"]:
            return result

        if signal == "NONE":
            if trend == "Bullish" and adx >= 25 and price > vwap:
                result["signal"] = "CALL"
                result["entry"] = "Trend Continuation"
                confidence += 15
                result["notes"] = "Aggressive BUY"
            elif trend == "Bearish" and adx >= 25 and price < vwap:
                result["signal"] = "PUT"
                result["entry"] = "Trend Continuation"
                confidence += 15
                result["notes"] = "Aggressive SELL"

        if result.get("signal") != "NONE" and trend != htf_trend and htf_trend != "Neutral":
            confidence -= 5

        if result.get("signal") != "NONE":
            if result.get("volume_breakout") is False:
                confidence += 5
            if result.get("strength") == "❌ WEAK":
                confidence += 10

        confidence = max(min(confidence, 95), 50)
        result["confidence"] = confidence

        if confidence >= 85:
            result["strength"] = "🔥 EXTREME"
        elif confidence >= 75:
            result["strength"] = "💪 STRONG"
        elif confidence >= 60:
            result["strength"] = "⚠ MODERATE"
        else:
            result["strength"] = "❌ WEAK"

        return result
    except Exception as e:
        logger.warning("Aggressive mode error: %s", e)
        return result


_original_cache_scan_results = cache_scan_results

def cache_scan_results(results: list):
    logger.info("AGGRESSIVE MODE ACTIVE")
    upgraded = [aggressive_signal_boost(r) for r in results]
    _original_cache_scan_results(upgraded)


def get_best_signal_result(results: list) -> dict:
    valid = [
        r for r in results
        if r.get("signal") != "NONE"
        and r.get("expected_move") not in [
            "Market Closed",
            "Avoid / Low Probability",
            "Avoid This Session",
            "Unknown",
            "Blocked - Stale Data",
            "Avoid / No Trade Day",
            "Adaptive Risk Lock",
            "Adaptive Filtered",
        ]
    ]

    if not valid:
        return {}

    valid.sort(
        key=lambda x: (
            x.get("confidence", 0),
            1 if x.get("entry") == "Enter Now" else 0,
            x.get("price", 0),
        ),
        reverse=True,
    )
    return valid[0]


def build_best_trade_message(result: dict) -> str:
    if not result:
        return "❌ No valid trade setup found in last scan"

    msg = [
        "🏆 BEST TRADE SETUP",
        "",
        f"Index: {result.get('name', 'UNKNOWN')}",
        f"Signal: {result.get('signal', 'NONE')}",
        f"Confidence: {result.get('confidence', 0)}%",
        f"Strength: {result.get('strength', '❌ WEAK')}",
        f"Entry: {result.get('entry', 'No Trade')}",
        f"Trigger Price: {result.get('entry_trigger_price', 0)}",
        f"Trend: {result.get('trend', 'Neutral')}",
        f"Higher TF Trend: {result.get('htf_trend', 'Neutral')}",
        f"Regime: {result.get('regime', 'UNKNOWN')}",
        f"Price: {result.get('price', 0)}",
        f"SL: {result.get('stop_loss', 0)}",
        f"Target: {result.get('target_price', 0)}",
        f"Trailing SL: {result.get('trailing_sl', 0)}",
        f"Expected Move: {result.get('expected_move', 'Unknown')}",
        f"ADX: {result.get('adx', 0)}",
        f"VWAP: {result.get('vwap', 0)}",
    ]

    strikes = result.get("strikes", {})
    if strikes:
        msg.extend([
            "",
            "🎯 Option Strikes:",
            f"ATM: {strikes.get('ATM', '-')}",
            f"ITM: {strikes.get('ITM', '-')}",
            f"OTM: {strikes.get('OTM', '-')}",
        ])

    return "\n".join(msg)


# =========================================================
# MARKET LOGIC
# =========================================================
def market_bias_proxy(close: pd.Series, ema20: pd.Series, ema50: pd.Series) -> Tuple[str, float]:
    if len(close) < 5 or len(ema20) < 1 or len(ema50) < 1:
        return "Neutral", 1.0

    price = safe_float(close.iloc[-1])
    prev_close = safe_float(close.iloc[-2], price)
    ema20_last = safe_float(ema20.iloc[-1])
    ema50_last = safe_float(ema50.iloc[-1])

    bullish = 0
    bearish = 0

    if price > ema20_last:
        bullish += 1
    elif price < ema20_last:
        bearish += 1

    if ema20_last > ema50_last:
        bullish += 1
    elif ema20_last < ema50_last:
        bearish += 1

    if price > prev_close:
        bullish += 1
    elif price < prev_close:
        bearish += 1

    if bullish >= 2 and bullish > bearish:
        return "Bullish", 0.90
    if bearish >= 2 and bearish > bullish:
        return "Bearish", 1.10
    return "Neutral", 1.00


def liquidity_map(high: pd.Series, low: pd.Series, close: pd.Series):
    if len(high) < 96 or len(low) < 96 or len(close) < 1:
        return "Not enough data", 0.0

    prev_day_high = safe_float(high.iloc[-96:-48].max())
    prev_day_low = safe_float(low.iloc[-96:-48].min())
    price = safe_float(close.iloc[-1])

    dist_high = abs(prev_day_high - price)
    dist_low = abs(price - prev_day_low)

    if dist_high < dist_low:
        return "Previous Day High", round(dist_high, 2)
    return "Previous Day Low", round(dist_low, 2)


def get_session_info():
    now = datetime.now(IST)
    current_time = now.time()

    market_open = dt_time(9, 15)
    market_close = dt_time(15, 30)

    if current_time < market_open or current_time > market_close:
        return "MARKET CLOSED", -50, False

    if dt_time(9, 15) <= current_time < dt_time(9, 45):
        return "OPENING VOLATILITY", 5, True

    if dt_time(9, 45) <= current_time < dt_time(11, 30):
        return "PRIME TREND WINDOW", 20, True

    if dt_time(11, 30) <= current_time < dt_time(13, 15):
        return "MIDDAY CHOP", -20, False

    if dt_time(13, 15) <= current_time < dt_time(14, 30):
        return "AFTERNOON BUILDUP", 10, True

    if dt_time(14, 30) <= current_time <= dt_time(15, 30):
        return "CLOSING MOVE WINDOW", 20, True

    return "UNKNOWN SESSION", 0, False


def calculate_atr_proxy(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(length, min_periods=length).mean()
    return atr


def calculate_adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    try:
        return ADXIndicator(high=high, low=low, close=close, window=length).adx()
    except Exception:
        return pd.Series([0.0] * len(close), index=close.index)


def calculate_vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    try:
        typical_price = (high + low + close) / 3
        volume_safe = volume.replace(0, pd.NA).ffill().fillna(1)
        return (typical_price * volume_safe).cumsum() / volume_safe.cumsum()
    except Exception:
        return pd.Series([0.0] * len(close), index=close.index)


def is_volume_breakout(volume: pd.Series) -> bool:
    try:
        if len(volume) < 20:
            return False
        recent_vol = safe_float(volume.iloc[-1], 0)
        avg_vol = safe_float(volume.iloc[-20:-1].mean(), 0)
        if avg_vol <= 0:
            return False
        return recent_vol > avg_vol * 1.5
    except Exception:
        return False


def get_opening_range(data: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
    try:
        if data is None or data.empty or len(data) < 3:
            return None, None

        or_high = safe_float(data["High"].iloc[:3].max(), 0)
        or_low = safe_float(data["Low"].iloc[:3].min(), 0)
        return or_high, or_low
    except Exception:
        return None, None


def check_orb_breakout(price: float, or_high: Optional[float], or_low: Optional[float]) -> Tuple[bool, str]:
    if or_high is None or or_low is None:
        return False, "NONE"

    if price > or_high:
        return True, "CALL"
    if price < or_low:
        return True, "PUT"
    return False, "NONE"


def vwap_behavior(close: pd.Series, vwap: pd.Series) -> str:
    try:
        if len(close) < 3 or len(vwap) < 3:
            return "NEUTRAL"

        c1 = safe_float(close.iloc[-2])
        c2 = safe_float(close.iloc[-1])
        v1 = safe_float(vwap.iloc[-2])
        v2 = safe_float(vwap.iloc[-1])

        if c1 < v1 and c2 > v2:
            return "RECLAIM"

        if c1 > v1 and c2 < v2:
            return "REJECTION"

        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


def is_strong_candle(open_price: float, high_price: float, low_price: float, close_price: float) -> bool:
    try:
        body = abs(close_price - open_price)
        candle_range = high_price - low_price

        if candle_range <= 0:
            return False

        body_ratio = body / candle_range
        upper_wick = high_price - max(open_price, close_price)
        lower_wick = min(open_price, close_price) - low_price

        if body_ratio > 0.6 and upper_wick < body and lower_wick < body:
            return True

        return False
    except Exception:
        return False


def detect_breakout_confirmation(close: pd.Series, high: pd.Series, low: pd.Series) -> Tuple[bool, str]:
    if len(close) < 22 or len(high) < 22 or len(low) < 22:
        return False, "NONE"

    price = safe_float(close.iloc[-1])
    recent_high = safe_float(high.iloc[-21:-1].max())
    recent_low = safe_float(low.iloc[-21:-1].min())

    if price > recent_high:
        return True, "CALL"
    if price < recent_low:
        return True, "PUT"
    return False, "NONE"


def detect_momentum_alignment(trend: str, momentum: float) -> bool:
    if trend == "Bullish" and momentum >= 52:
        return True
    if trend == "Bearish" and momentum <= 48:
        return True
    return False


def get_higher_timeframe_trend(symbol: str) -> str:
    try:
        data = yf.download(
            symbol,
            interval=HIGHER_TF_INTERVAL,
            period=HIGHER_TF_PERIOD,
            auto_adjust=False,
            progress=False,
            threads=False,
        )

        data = normalize_ohlc_dataframe(data)
        if data.empty:
            return "Neutral"

        close = extract_series(data, "Close")
        if len(close) < 50:
            return "Neutral"

        ema20 = EMAIndicator(close, window=20).ema_indicator()
        ema50 = EMAIndicator(close, window=50).ema_indicator()

        ema20_last = safe_float(ema20.iloc[-1])
        ema50_last = safe_float(ema50.iloc[-1])

        if ema20_last > ema50_last:
            return "Bullish"
        if ema20_last < ema50_last:
            return "Bearish"
        return "Neutral"
    except Exception:
        return "Neutral"


def detect_entry_candle(close: pd.Series, open_: pd.Series, high: pd.Series, low: pd.Series) -> str:
    if len(close) < 3 or len(open_) < 3:
        return "NONE"

    last_close = safe_float(close.iloc[-1])
    last_open = safe_float(open_.iloc[-1])
    prev_close = safe_float(close.iloc[-2])
    prev_open = safe_float(open_.iloc[-2])

    if last_close > last_open and prev_close < prev_open and last_close > prev_open:
        return "CALL"

    if last_close < last_open and prev_close > prev_open and last_close < prev_open:
        return "PUT"

    return "NONE"


def get_option_strike(price: float, symbol_name: str, signal: str) -> dict:
    try:
        step = 100 if "BANK" in symbol_name.upper() else 50
        atm = round(price / step) * step

        if signal == "CALL":
            return {"ATM": int(atm), "ITM": int(atm - step), "OTM": int(atm + step)}

        if signal == "PUT":
            return {"ATM": int(atm), "ITM": int(atm + step), "OTM": int(atm - step)}

        return {}
    except Exception:
        return {}


def get_entry_timing_and_trigger(signal: str, close: pd.Series, high: pd.Series, low: pd.Series) -> Tuple[str, float]:
    try:
        if len(close) < 2 or len(high) < 2 or len(low) < 2:
            return "Wait", safe_float(close.iloc[-1], 0)

        last_close = safe_float(close.iloc[-1])
        prev_high = safe_float(high.iloc[-2])
        prev_low = safe_float(low.iloc[-2])

        if signal == "CALL":
            if last_close > prev_high:
                return "Enter Now", last_close
            return "Wait Breakout", prev_high

        if signal == "PUT":
            if last_close < prev_low:
                return "Enter Now", last_close
            return "Wait Breakdown", prev_low

        return "No Trade", last_close
    except Exception:
        return "Wait", 0.0


def classify_signal_strength(confidence: int, regime: str) -> str:
    if confidence >= 85:
        return "🔥 EXTREME"
    if confidence >= 75:
        return "💪 STRONG"
    if confidence >= 60:
        return "⚠ MODERATE"
    return "❌ WEAK"


def apply_trailing_sl(signal: str, price: float, atr_value: float) -> float:
    try:
        trail = max(atr_value * 0.5, 30)

        if signal == "CALL":
            return round(price - trail, 2)

        if signal == "PUT":
            return round(price + trail, 2)

        return 0.0
    except Exception:
        return 0.0


def detect_regime(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    ema20: pd.Series,
    ema50: pd.Series,
    momentum: float,
    compression: bool,
    vol_expansion: bool,
    liquidity_trap: bool,
):
    if len(close) < 30 or len(high) < 30 or len(low) < 30:
        return "UNKNOWN", 0

    recent_high = safe_float(high.iloc[-20:].max())
    recent_low = safe_float(low.iloc[-20:].min())
    price = safe_float(close.iloc[-1])

    day_range = recent_high - recent_low
    avg_recent_bar_range = safe_float((high.iloc[-20:] - low.iloc[-20:]).mean())
    ema_gap = abs(safe_float(ema20.iloc[-1]) - safe_float(ema50.iloc[-1]))

    breakout_up = price >= recent_high * 0.999
    breakout_down = price <= recent_low * 1.001

    regime = "RANGE DAY"
    regime_score = 0

    if compression and vol_expansion and (breakout_up or breakout_down):
        regime = "BREAKOUT DAY"
        regime_score = 25
    elif ema_gap > avg_recent_bar_range * 0.6 and (momentum > 60 or momentum < 40):
        regime = "TREND DAY"
        regime_score = 25
    elif liquidity_trap:
        regime = "REVERSAL DAY"
        regime_score = 10
    elif day_range < avg_recent_bar_range * 8:
        regime = "RANGE DAY"
        regime_score = -10

    return regime, regime_score


def calculate_expected_move(
    confidence: int,
    regime: str,
    session_name: str,
    trading_allowed: bool,
    atr_value: float,
) -> str:
    if session_name == "MARKET CLOSED":
        return "Market Closed"
    if regime == "RANGE DAY":
        return "Avoid / Low Probability"
    if not trading_allowed:
        return "Avoid This Session"

    if atr_value >= 220 or confidence >= 88:
        return "300-500 points"
    if atr_value >= 130 or confidence >= 75:
        return "150-300 points"
    if atr_value >= 70 or confidence >= 60:
        return "80-150 points"
    return "Small"


def generate_signal(
    symbol: str,
    trend: str,
    momentum: float,
    regime: str,
    trading_allowed: bool,
    liquidity_trap: bool,
    trap_direction: str,
    session_name: str,
    breakout_confirmed: bool,
    breakout_direction: str,
    close: pd.Series,
    open_: pd.Series,
    high: pd.Series,
    low: pd.Series,
) -> str:
    if session_name == "MARKET CLOSED":
        return "NONE"

    if regime not in ["TREND DAY", "BREAKOUT DAY"]:
        return "NONE"

    if not trading_allowed:
        return "NONE"

    if not detect_momentum_alignment(trend, momentum):
        return "NONE"

    htf_trend = get_higher_timeframe_trend(symbol)
    if htf_trend != "Neutral" and trend != htf_trend:
        return "NONE"

    entry_signal = detect_entry_candle(close, open_, high, low)

    if regime == "BREAKOUT DAY":
        if breakout_confirmed and breakout_direction in ["CALL", "PUT"] and breakout_direction == entry_signal:
            return breakout_direction
        return "NONE"

    if regime == "TREND DAY":
        if liquidity_trap and trap_direction != "NONE":
            return "NONE"
        if trend == "Bullish" and momentum > 55 and entry_signal == "CALL":
            return "CALL"
        if trend == "Bearish" and momentum < 45 and entry_signal == "PUT":
            return "PUT"

    return "NONE"


def apply_top3_filters(
    signal: str,
    price: float,
    adx_value: float,
    vwap_value: float,
    volume_breakout: bool,
    orb_break: bool,
    orb_dir: str,
    vwap_state: str,
    strong_candle: bool,
) -> Tuple[str, int, list]:
    reasons = []
    confidence_adjustment = 0

    if signal == "NONE":
        return signal, confidence_adjustment, reasons

    if adx_value < 18:
        reasons.append("Weak Trend (ADX Low)")

    if signal == "CALL" and price < vwap_value:
        reasons.append("Below VWAP")

    if signal == "PUT" and price > vwap_value:
        reasons.append("Above VWAP")

    if not volume_breakout:
        reasons.append("No Volume Breakout")

    if orb_break and signal == orb_dir:
        confidence_adjustment += 10
        reasons.append("ORB Breakout")

    if signal == "CALL":
        if vwap_state == "RECLAIM":
            confidence_adjustment += 10
            reasons.append("VWAP Reclaim")
        else:
            reasons.append("Bad VWAP Behavior")

    if signal == "PUT":
        if vwap_state == "REJECTION":
            confidence_adjustment += 10
            reasons.append("VWAP Rejection")
        else:
            reasons.append("Bad VWAP Behavior")

    if not strong_candle:
        reasons.append("Weak Candle")

    blocking_reasons = [
        r for r in reasons
        if r in [
            "Weak Trend (ADX Low)",
            "Below VWAP",
            "Above VWAP",
            "No Volume Breakout",
            "Bad VWAP Behavior",
            "Weak Candle",
        ]
    ]

    if blocking_reasons:
        return "NONE", confidence_adjustment - 15, reasons

    return signal, confidence_adjustment + 10, reasons


def detect_no_trade_conditions(result: dict) -> Tuple[bool, list]:
    reasons = []

    atr_value = safe_float(result.get("atr", 0), 0)
    adx_value = safe_float(result.get("adx", 0), 0)
    confidence = safe_float(result.get("confidence", 0), 0)
    regime = str(result.get("regime", "UNKNOWN"))
    session_name = str(result.get("session", "UNKNOWN"))
    vwap_state = str(result.get("vwap_behavior", "NEUTRAL"))
    volume_breakout = bool(result.get("volume_breakout", False))
    strong_candle = bool(result.get("strong_candle", False))
    orb_break = bool(result.get("orb_break", False))
    trend = str(result.get("trend", "Neutral"))
    htf_trend = str(result.get("htf_trend", "Neutral"))

    if session_name in ["MARKET CLOSED", "MIDDAY CHOP", "STALE DATA"]:
        reasons.append(f"Bad Session: {session_name}")

    if regime == "RANGE DAY":
        reasons.append("Range Day")

    if atr_value < 45:
        reasons.append("ATR Too Low")

    if adx_value < 16:
        reasons.append("ADX Too Weak")

    if confidence < max(ALERT_THRESHOLD - 10, 55):
        reasons.append("Confidence Too Low")

    if vwap_state == "NEUTRAL":
        reasons.append("VWAP Sideways")

    if not volume_breakout:
        reasons.append("No Volume Expansion")

    if not strong_candle:
        reasons.append("Weak Candle")

    if trend != "Neutral" and htf_trend != "Neutral" and trend != htf_trend:
        reasons.append("Trend Misaligned With HTF")

    if regime == "BREAKOUT DAY" and not orb_break:
        reasons.append("No ORB Breakout")

    no_trade = len(reasons) >= 3
    return no_trade, reasons


def get_recent_trade_metrics(lookback: int = 20) -> dict:
    df = load_trade_log_df()
    if df.empty:
        return {
            "total": 0,
            "closed": 0,
            "targets": 0,
            "stops": 0,
            "win_rate": 0.0,
            "avg_rr": 0.0,
            "consecutive_losses": 0,
        }

    recent = df.tail(lookback).copy()
    recent["rr"] = pd.to_numeric(recent["rr"], errors="coerce")

    closed = recent[recent["status"] == "CLOSED"].copy()
    targets = len(closed[closed["outcome"] == "TARGET_HIT"])
    stops = len(closed[closed["outcome"].isin(["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"])])
    win_rate = round((targets / len(closed)) * 100, 2) if len(closed) > 0 else 0.0
    avg_rr = round(closed["rr"].dropna().mean(), 2) if closed["rr"].notna().any() else 0.0

    consecutive_losses = 0
    for _, row in closed.iloc[::-1].iterrows():
        if row["outcome"] in ["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"]:
            consecutive_losses += 1
        else:
            break

    return {
        "total": len(recent),
        "closed": len(closed),
        "targets": targets,
        "stops": stops,
        "win_rate": win_rate,
        "avg_rr": avg_rr,
        "consecutive_losses": consecutive_losses,
    }


def get_adaptive_settings() -> dict:
    metrics = get_recent_trade_metrics(lookback=20)

    threshold_boost = 0
    confidence_penalty = 0
    hard_block = False
    notes = []

    if metrics["consecutive_losses"] >= 3:
        hard_block = True
        notes.append("3 Consecutive Losses")

    if metrics["closed"] >= 5 and metrics["win_rate"] < 35:
        threshold_boost += 10
        confidence_penalty += 10
        notes.append("Low Recent Win Rate")

    if metrics["avg_rr"] < 0 and metrics["closed"] >= 5:
        threshold_boost += 5
        confidence_penalty += 5
        notes.append("Negative Recent RR")

    if metrics["closed"] >= 5 and metrics["win_rate"] >= 60:
        threshold_boost -= 5
        notes.append("Good Recent Win Rate")

    return {
        "metrics": metrics,
        "threshold_boost": threshold_boost,
        "confidence_penalty": confidence_penalty,
        "hard_block": hard_block,
        "notes": notes,
    }


def calculate_sl_tp(signal: str, price: float, atr_value: float) -> Tuple[float, float]:
    atr_buffer = max(atr_value * 0.8, 40)

    if signal == "CALL":
        return round(price - atr_buffer, 2), round(price + atr_buffer * 1.8, 2)

    if signal == "PUT":
        return round(price + atr_buffer, 2), round(price - atr_buffer * 1.8, 2)

    return 0.0, 0.0


# =========================================================
# DATA FETCH + ANALYSIS
# =========================================================
def fetch_market_data(symbol: str) -> pd.DataFrame:
    try:
        data = yf.download(
            symbol,
            interval=DEFAULT_INTERVAL,
            period=DEFAULT_PERIOD,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return normalize_ohlc_dataframe(data)
    except Exception as e:
        logger.exception("Failed to fetch data for %s: %s", symbol, e)
        return pd.DataFrame()


def analyze_market(symbol: str, name: str) -> dict:
    data = fetch_market_data(symbol)

    default_result = {
        "name": name,
        "message": f"{name}\n\nData unavailable",
        "signal": "NONE",
        "confidence": 0,
        "expected_move": "Unknown",
        "regime": "UNKNOWN",
        "session": "UNKNOWN",
        "price": 0,
        "trend": "Neutral",
        "rsi": 0,
        "ema20": 0,
        "ema50": 0,
        "compression": False,
        "vol_expansion": False,
        "liquidity_trap": False,
        "options_bias": "Neutral",
        "pcr": 1.0,
        "liquidity_target": "Unknown",
        "distance": 0,
        "atr": 0,
        "adx": 0,
        "vwap": 0,
        "volume_breakout": False,
        "breakout_confirmed": False,
        "breakout_direction": "NONE",
        "orb_break": False,
        "orb_direction": "NONE",
        "vwap_behavior": "NEUTRAL",
        "strong_candle": False,
        "stop_loss": 0,
        "target_price": 0,
        "strength": "❌ WEAK",
        "entry": "No Trade",
        "entry_trigger_price": 0,
        "strikes": {},
        "trailing_sl": 0,
        "htf_trend": "Neutral",
        "no_trade_day": False,
        "no_trade_reasons": [],
        "adaptive_notes": [],
        "adaptive_threshold": ALERT_THRESHOLD,
    }

    if data.empty:
        return default_result

    if is_data_stale(data):
        stale_age = round(get_data_age_minutes(data), 2)
        default_result["message"] = (
            f"{name}\n\n"
            f"Data appears stale during market hours\n"
            f"Last candle age: {stale_age} minutes\n"
            f"Signal blocked for safety"
        )
        default_result["expected_move"] = "Blocked - Stale Data"
        default_result["session"] = "STALE DATA"
        return default_result

    close = extract_series(data, "Close")
    open_ = extract_series(data, "Open")
    high = extract_series(data, "High")
    low = extract_series(data, "Low")

    if "Volume" in data.columns:
        volume = extract_series(data, "Volume")
    else:
        volume = pd.Series([0.0] * len(close), index=close.index)

    min_len = min(len(close), len(open_), len(high), len(low), len(volume))
    close = close.iloc[-min_len:]
    open_ = open_.iloc[-min_len:]
    high = high.iloc[-min_len:]
    low = low.iloc[-min_len:]
    volume = volume.iloc[-min_len:]

    if min_len < MIN_REQUIRED_BARS:
        default_result["message"] = f"{name}\n\nNot enough data for analysis"
        return default_result

    rsi = RSIIndicator(close, window=14).rsi()
    ema20 = EMAIndicator(close, window=20).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    atr_series = calculate_atr_proxy(high, low, close, length=14)
    adx_series = calculate_adx(high, low, close, length=14)
    vwap_series = calculate_vwap(high, low, close, volume)

    price = safe_float(close.iloc[-1])
    ema20_last = safe_float(ema20.iloc[-1])
    ema50_last = safe_float(ema50.iloc[-1])
    momentum = safe_float(rsi.iloc[-1], 50)
    atr_value = safe_float(atr_series.iloc[-1], 0)
    adx_value = safe_float(adx_series.iloc[-1], 0)
    vwap_value = safe_float(vwap_series.iloc[-1], 0)
    volume_breakout = is_volume_breakout(volume)

    trend = "Neutral"
    trend_score = 0
    if ema20_last > ema50_last:
        trend = "Bullish"
        trend_score = 25
    elif ema20_last < ema50_last:
        trend = "Bearish"
        trend_score = 25

    momentum_score = 0
    if trend == "Bullish" and momentum >= 52:
        momentum_score = 20
    elif trend == "Bearish" and momentum <= 48:
        momentum_score = 20

    recent_range = safe_float((high.iloc[-5:] - low.iloc[-5:]).mean())
    past_range = safe_float((high.iloc[-20:-5] - low.iloc[-20:-5]).mean())
    compression = recent_range < (past_range * 0.6) if past_range > 0 else False
    compression_score = 12 if compression else 0

    vol_recent = safe_float((high.iloc[-3:] - low.iloc[-3:]).mean())
    vol_prev = safe_float((high.iloc[-10:-3] - low.iloc[-10:-3]).mean())
    vol_expansion = vol_recent > (vol_prev * 1.4) if vol_prev > 0 else False
    vol_score = 18 if vol_expansion else 0

    last_high = safe_float(high.iloc[-1])
    last_low = safe_float(low.iloc[-1])
    prev_high = safe_float(high.iloc[-10:-1].max())
    prev_low = safe_float(low.iloc[-10:-1].min())

    liquidity_trap = False
    trap_direction = "NONE"
    trap_score = 0

    if last_high > prev_high and price < prev_high:
        liquidity_trap = True
        trap_direction = "PUT"
        trap_score = 8
    elif last_low < prev_low and price > prev_low:
        liquidity_trap = True
        trap_direction = "CALL"
        trap_score = 8

    options_bias, pcr = market_bias_proxy(close, ema20, ema50)
    option_score = 0
    if options_bias == trend and trend != "Neutral":
        option_score = 10
    elif options_bias != "Neutral":
        option_score = 3

    target, distance = liquidity_map(high, low, close)
    liquidity_score = 15 if distance < 200 else 0

    breakout_confirmed, breakout_direction = detect_breakout_confirmation(close, high, low)
    breakout_score = 12 if breakout_confirmed else 0

    or_high, or_low = get_opening_range(data)
    orb_break, orb_direction = check_orb_breakout(price, or_high, or_low)
    vwap_state = vwap_behavior(close, vwap_series)
    strong_candle = is_strong_candle(
        safe_float(open_.iloc[-1]),
        safe_float(high.iloc[-1]),
        safe_float(low.iloc[-1]),
        safe_float(close.iloc[-1]),
    )

    regime, regime_score = detect_regime(
        close=close,
        high=high,
        low=low,
        ema20=ema20,
        ema50=ema50,
        momentum=momentum,
        compression=compression,
        vol_expansion=vol_expansion,
        liquidity_trap=liquidity_trap,
    )

    session_name, session_score, trading_allowed = get_session_info()
    htf_trend = get_higher_timeframe_trend(symbol)

    confidence = (
        trend_score
        + momentum_score
        + compression_score
        + vol_score
        + trap_score
        + option_score
        + liquidity_score
        + breakout_score
        + regime_score
        + session_score
    )
    confidence = max(0, min(confidence, 100))

    signal = generate_signal(
        symbol=symbol,
        trend=trend,
        momentum=momentum,
        regime=regime,
        trading_allowed=trading_allowed,
        liquidity_trap=liquidity_trap,
        trap_direction=trap_direction,
        session_name=session_name,
        breakout_confirmed=breakout_confirmed,
        breakout_direction=breakout_direction,
        close=close,
        open_=open_,
        high=high,
        low=low,
    )

    signal, confidence_adjustment, filter_notes = apply_top3_filters(
        signal=signal,
        price=price,
        adx_value=adx_value,
        vwap_value=vwap_value,
        volume_breakout=volume_breakout,
        orb_break=orb_break,
        orb_dir=orb_direction,
        vwap_state=vwap_state,
        strong_candle=strong_candle,
    )
    confidence = max(0, min(100, confidence + confidence_adjustment))

    expected_move = calculate_expected_move(
        confidence=confidence,
        regime=regime,
        session_name=session_name,
        trading_allowed=trading_allowed,
        atr_value=atr_value,
    )

    if expected_move in ["Market Closed", "Avoid / Low Probability", "Avoid This Session"]:
        signal = "NONE"

    stop_loss, target_price = calculate_sl_tp(signal, price, atr_value)
    strength = classify_signal_strength(confidence, regime)
    entry, entry_trigger_price = get_entry_timing_and_trigger(signal, close, high, low)
    strikes = get_option_strike(price, name, signal)
    trailing_sl = apply_trailing_sl(signal, price, atr_value)
    ultra_confirmation = "YES" if signal != "NONE" else "NO"

    result = {
        "name": name,
        "message": "",
        "signal": signal,
        "confidence": int(confidence),
        "expected_move": expected_move,
        "regime": regime,
        "session": session_name,
        "price": round(price, 2),
        "trend": trend,
        "rsi": round(momentum, 2),
        "ema20": round(ema20_last, 2),
        "ema50": round(ema50_last, 2),
        "compression": compression,
        "vol_expansion": vol_expansion,
        "liquidity_trap": liquidity_trap,
        "options_bias": options_bias,
        "pcr": pcr,
        "liquidity_target": target,
        "distance": distance,
        "atr": round(atr_value, 2),
        "adx": round(adx_value, 2),
        "vwap": round(vwap_value, 2),
        "volume_breakout": volume_breakout,
        "breakout_confirmed": breakout_confirmed,
        "breakout_direction": breakout_direction,
        "orb_break": orb_break,
        "orb_direction": orb_direction,
        "vwap_behavior": vwap_state,
        "strong_candle": strong_candle,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "strength": strength,
        "entry": entry,
        "entry_trigger_price": round(entry_trigger_price, 2),
        "strikes": strikes,
        "trailing_sl": trailing_sl,
        "htf_trend": htf_trend,
        "no_trade_day": False,
        "no_trade_reasons": [],
        "adaptive_notes": [],
        "adaptive_threshold": ALERT_THRESHOLD,
    }

    no_trade, no_trade_reasons = detect_no_trade_conditions(result)
    if no_trade:
        result["signal"] = "NONE"
        result["strength"] = "❌ WEAK"
        result["entry"] = "No Trade"
        result["expected_move"] = "Avoid / No Trade Day"
        result["stop_loss"] = 0
        result["target_price"] = 0
        result["trailing_sl"] = 0
        result["entry_trigger_price"] = 0
        result["strikes"] = {}
        result["no_trade_day"] = True
        result["no_trade_reasons"] = no_trade_reasons

    adaptive = get_adaptive_settings()
    result["adaptive_notes"] = adaptive["notes"]
    result["adaptive_threshold"] = ALERT_THRESHOLD + adaptive["threshold_boost"]

    if adaptive["hard_block"]:
        result["signal"] = "NONE"
        result["strength"] = "❌ WEAK"
        result["entry"] = "No Trade"
        result["expected_move"] = "Adaptive Risk Lock"
        result["stop_loss"] = 0
        result["target_price"] = 0
        result["trailing_sl"] = 0
        result["entry_trigger_price"] = 0
        result["strikes"] = {}
    elif adaptive["confidence_penalty"] > 0:
        result["confidence"] = max(0, int(result["confidence"] - adaptive["confidence_penalty"]))
        if result["confidence"] < result["adaptive_threshold"]:
            result["signal"] = "NONE"
            result["strength"] = "❌ WEAK"
            result["entry"] = "No Trade"
            result["expected_move"] = "Adaptive Filtered"
            result["stop_loss"] = 0
            result["target_price"] = 0
            result["trailing_sl"] = 0
            result["entry_trigger_price"] = 0
            result["strikes"] = {}

    msg = (
        f"{name}\n\n"
        f"Price: {result['price']}\n"
        f"Market Regime: {result['regime']}\n"
        f"Session: {result['session']}\n\n"
        f"Trend: {result['trend']}\n"
        f"Higher TF Trend: {result['htf_trend']}\n"
        f"RSI: {result['rsi']}\n"
        f"EMA20: {result['ema20']}\n"
        f"EMA50: {result['ema50']}\n"
        f"ATR: {result['atr']}\n"
        f"ADX: {result['adx']}\n"
        f"VWAP: {result['vwap']}\n\n"
        f"Compression: {result['compression']}\n"
        f"Vol Expansion: {result['vol_expansion']}\n"
        f"Volume Breakout: {result['volume_breakout']}\n"
        f"Liquidity Trap: {result['liquidity_trap']}\n"
        f"Breakout Confirmed: {result['breakout_confirmed']}\n"
        f"Breakout Direction: {result['breakout_direction']}\n"
        f"ORB: {result['orb_break']} ({result['orb_direction']})\n"
        f"VWAP Behavior: {result['vwap_behavior']}\n"
        f"Strong Candle: {result['strong_candle']}\n\n"
        f"Market Bias Proxy: {result['options_bias']}\n"
        f"PCR Proxy: {result['pcr']}\n\n"
        f"Liquidity Target: {result['liquidity_target']}\n"
        f"Distance: {result['distance']} pts\n\n"
        f"Confidence: {result['confidence']}%\n"
        f"Signal: {result['signal']}\n"
        f"Expected Move: {result['expected_move']}\n"
        f"Stop Loss: {result['stop_loss']}\n"
        f"Target: {result['target_price']}\n"
        f"Trailing SL: {result['trailing_sl']}\n"
        f"Strength: {result['strength']}\n"
        f"Entry: {result['entry']}\n"
        f"Trigger Price: {result['entry_trigger_price']}\n"
        f"Ultra Confirmation: {ultra_confirmation}"
    )

    if filter_notes:
        msg += f"\n\n🧠 Logic: {', '.join(filter_notes)}"

    if result["no_trade_day"]:
        msg += f"\n\n🚫 NO TRADE DAY\nReason: {', '.join(result['no_trade_reasons'])}"

    if adaptive["hard_block"]:
        msg += f"\n\n🛑 ADAPTIVE RISK LOCK ACTIVE\nReason: {', '.join(adaptive['notes']) if adaptive['notes'] else 'Risk Protection'}"
    elif adaptive["confidence_penalty"] > 0:
        msg += f"\n\n⚙ Adaptive Adjustment: -{adaptive['confidence_penalty']} confidence"
        if adaptive["notes"]:
            msg += f"\nAdaptive Reason: {', '.join(adaptive['notes'])}"
    elif adaptive["threshold_boost"] < 0 and adaptive["notes"]:
        msg += f"\n\n⚙ Adaptive Adjustment: Aggressive Mode ({', '.join(adaptive['notes'])})"

    if result["signal"] != "NONE" and result["strikes"]:
        msg += (
            f"\n\n🎯 Option Strikes:\n"
            f"ATM: {result['strikes']['ATM']}\n"
            f"ITM: {result['strikes']['ITM']}\n"
            f"OTM: {result['strikes']['OTM']}"
        )

    result["message"] = msg
    return result


# =========================================================
# OUTCOME TRACKER
# =========================================================
def fetch_outcome_data(symbol: str) -> pd.DataFrame:
    try:
        data = yf.download(
            symbol,
            interval=DEFAULT_INTERVAL,
            period=DEFAULT_PERIOD,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return normalize_ohlc_dataframe(data)
    except Exception as e:
        logger.warning("Failed outcome fetch for %s: %s", symbol, e)
        return pd.DataFrame()


def compute_rr(signal: str, entry_price: float, stop_loss: float, exit_price: float) -> float:
    try:
        if signal == "CALL":
            risk = entry_price - stop_loss
            reward = exit_price - entry_price
        else:
            risk = stop_loss - entry_price
            reward = entry_price - exit_price

        if risk <= 0:
            return 0.0
        return round(reward / risk, 2)
    except Exception:
        return 0.0


def update_trade_outcomes():
    try:
        df = load_trade_log_df()
        if df.empty:
            return

        changed = False
        now = datetime.now(IST)

        for idx, row in df.iterrows():
            status = str(row.get("status", ""))
            if status not in ["PENDING", "ENTERED"]:
                continue

            signal = str(row.get("signal", "NONE"))
            yahoo_symbol = str(row.get("yahoo_symbol", ""))
            if signal not in ["CALL", "PUT"] or not yahoo_symbol:
                continue

            signal_time_str = str(row.get("signal_time", ""))
            try:
                signal_time = datetime.fromisoformat(signal_time_str)
            except Exception:
                continue

            if signal_time.tzinfo is None:
                signal_time = signal_time.replace(tzinfo=IST)
            else:
                signal_time = signal_time.astimezone(IST)

            if now - signal_time > timedelta(days=TRADE_MAX_AGE_DAYS):
                if status == "PENDING":
                    df.at[idx, "status"] = "EXPIRED"
                    df.at[idx, "outcome"] = "NOT_TRIGGERED"
                    df.at[idx, "notes"] = "Trade expired without trigger"
                    changed = True
                elif status == "ENTERED" and pd.isna(row.get("exit_time")):
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "TIME_EXIT"
                    changed = True
                continue

            market_data = fetch_outcome_data(yahoo_symbol)
            if market_data.empty:
                continue

            if market_data.index.tz is None:
                market_data.index = market_data.index.tz_localize(IST)
            else:
                market_data.index = market_data.index.tz_convert(IST)

            future = market_data[market_data.index > signal_time]
            if future.empty:
                continue

            trigger_price = safe_float(row.get("trigger_price", row.get("price_at_signal", 0)))
            stop_loss = safe_float(row.get("stop_loss", 0))
            target_price = safe_float(row.get("target_price", 0))
            entry_price_row = row.get("entry_price", "")
            entry_time_row = row.get("entry_time", "")

            entered = status == "ENTERED" and str(entry_time_row).strip() != ""

            entry_price = safe_float(entry_price_row, 0) if entered else 0.0
            entry_time = None
            if entered:
                try:
                    entry_time = datetime.fromisoformat(str(entry_time_row))
                    if entry_time.tzinfo is None:
                        entry_time = entry_time.replace(tzinfo=IST)
                    else:
                        entry_time = entry_time.astimezone(IST)
                except Exception:
                    entered = False

            if not entered:
                for ts, candle in future.iterrows():
                    high_val = safe_float(candle["High"])
                    low_val = safe_float(candle["Low"])

                    if signal == "CALL" and high_val >= trigger_price:
                        entry_price = trigger_price
                        entry_time = ts.to_pydatetime()
                        df.at[idx, "status"] = "ENTERED"
                        df.at[idx, "entry_price"] = entry_price
                        df.at[idx, "entry_time"] = entry_time.isoformat()
                        changed = True
                        entered = True
                        break

                    if signal == "PUT" and low_val <= trigger_price:
                        entry_price = trigger_price
                        entry_time = ts.to_pydatetime()
                        df.at[idx, "status"] = "ENTERED"
                        df.at[idx, "entry_price"] = entry_price
                        df.at[idx, "entry_time"] = entry_time.isoformat()
                        changed = True
                        entered = True
                        break

            if not entered or entry_time is None:
                continue

            post_entry = market_data[market_data.index >= entry_time]
            if post_entry.empty:
                continue

            for ts, candle in post_entry.iterrows():
                high_val = safe_float(candle["High"])
                low_val = safe_float(candle["Low"])
                close_val = safe_float(candle["Close"])

                if signal == "CALL":
                    sl_hit = low_val <= stop_loss
                    tp_hit = high_val >= target_price
                else:
                    sl_hit = high_val >= stop_loss
                    tp_hit = low_val <= target_price

                if sl_hit and tp_hit:
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "SL_FIRST_SAME_CANDLE"
                    df.at[idx, "exit_price"] = stop_loss
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, stop_loss)
                    df.at[idx, "notes"] = "Both SL and TP touched same candle; conservative SL assumed"
                    changed = True
                    break

                if sl_hit:
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "STOP_LOSS_HIT"
                    df.at[idx, "exit_price"] = stop_loss
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, stop_loss)
                    changed = True
                    break

                if tp_hit:
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "TARGET_HIT"
                    df.at[idx, "exit_price"] = target_price
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, target_price)
                    changed = True
                    break

                if now - entry_time > timedelta(days=1):
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "TIME_EXIT"
                    df.at[idx, "exit_price"] = close_val
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, close_val)
                    df.at[idx, "notes"] = "Closed by time-based exit"
                    changed = True
                    break

        if changed:
            save_trade_log_df(df)

    except Exception as e:
        logger.warning("Outcome tracker failed: %s", e)


def get_daily_stats_message() -> str:
    df = load_trade_log_df()
    if df.empty:
        return "📊 DAILY STATS\n\nNo trade data yet"

    df["signal_time"] = pd.to_datetime(df["signal_time"], errors="coerce")
    if str(df["signal_time"].dt.tz) == "None":
        df["signal_time"] = df["signal_time"].dt.tz_localize(IST)
    else:
        df["signal_time"] = df["signal_time"].dt.tz_convert(IST)

    today = datetime.now(IST).date()
    today_df = df[df["signal_time"].dt.date == today].copy()

    if today_df.empty:
        return "📊 DAILY STATS\n\nNo signals today"

    total = len(today_df)
    entered = len(today_df[today_df["status"] == "ENTERED"])
    closed = len(today_df[today_df["status"] == "CLOSED"])
    expired = len(today_df[today_df["status"] == "EXPIRED"])
    targets = len(today_df[today_df["outcome"] == "TARGET_HIT"])
    stops = len(today_df[today_df["outcome"].isin(["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"])])
    time_exits = len(today_df[today_df["outcome"] == "TIME_EXIT"])

    rr_series = pd.to_numeric(today_df["rr"], errors="coerce")
    avg_rr = round(rr_series.dropna().mean(), 2) if rr_series.notna().any() else 0.0

    win_rate = round((targets / closed) * 100, 2) if closed > 0 else 0.0

    counts_by_symbol = today_df.groupby("symbol").size().sort_values(ascending=False)
    best_symbol = counts_by_symbol.index[0] if not counts_by_symbol.empty else "N/A"

    return (
        "📊 DAILY STATS\n\n"
        f"Signals Today: {total}\n"
        f"Entered: {entered}\n"
        f"Closed: {closed}\n"
        f"Expired: {expired}\n"
        f"Target Hits: {targets}\n"
        f"Stop Loss Hits: {stops}\n"
        f"Time Exits: {time_exits}\n"
        f"Win Rate: {win_rate}%\n"
        f"Average RR: {avg_rr}\n"
        f"Most Active Index: {best_symbol}"
    )


# =========================================================
# ALERTS
# =========================================================
def build_signal_summary(result: dict) -> str:
    return (
        f"{result['name']}\n"
        f"Price: {result['price']}\n"
        f"Trend: {result['trend']}\n"
        f"Higher TF Trend: {result['htf_trend']}\n"
        f"Regime: {result['regime']}\n"
        f"Session: {result['session']}\n"
        f"Signal: {result['signal']}\n"
        f"Confidence: {result['confidence']}%\n"
        f"Expected Move: {result['expected_move']}\n"
        f"Strength: {result['strength']}\n"
        f"Entry: {result['entry']}\n"
        f"Trigger: {result['entry_trigger_price']}\n"
        f"ADX: {result['adx']}\n"
        f"VWAP: {result['vwap']}\n"
        f"Volume Breakout: {result['volume_breakout']}\n"
        f"SL: {result['stop_loss']}\n"
        f"Target: {result['target_price']}\n"
        f"Trailing SL: {result['trailing_sl']}"
    )


def should_send_alert(result: dict) -> bool:
    if result.get("session") == "STALE DATA":
        return False
    if result.get("no_trade_day", False):
        return False
    if result.get("signal") == "NONE":
        return False
    if result.get("strength") == "❌ WEAK":
        return False
    if safe_float(result.get("confidence", 0), 0) < safe_float(result.get("adaptive_threshold", ALERT_THRESHOLD), ALERT_THRESHOLD):
        return False
    if result.get("expected_move") in [
        "Market Closed",
        "Avoid / Low Probability",
        "Avoid This Session",
        "Unknown",
        "Blocked - Stale Data",
        "Avoid / No Trade Day",
        "Adaptive Risk Lock",
        "Adaptive Filtered",
    ]:
        return False
    if result.get("entry") not in ["Enter Now", "Wait Breakout", "Wait Breakdown"]:
        return False
    return True


def process_alert_logging(result: dict):
    try:
        log_trade(result)
    except Exception as e:
        logger.warning("Alert logging failed: %s", e)


def is_duplicate_alert(result: dict) -> bool:
    key = f"{result['name']}|{result['signal']}|{result['regime']}"
    now = datetime.now(IST)

    if key not in LAST_ALERTS:
        LAST_ALERTS[key] = now
        save_alert_state()
        return False

    last_time = LAST_ALERTS[key]
    if now - last_time < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
        return True

    LAST_ALERTS[key] = now
    save_alert_state()
    return False


# =========================================================
# SCAN RUNNER
# =========================================================
async def run_full_scan_once() -> list:
    results = [
        analyze_market(SYMBOLS["NIFTY 50"], "NIFTY 50"),
        analyze_market(SYMBOLS["BANK NIFTY"], "BANK NIFTY"),
        analyze_market(SYMBOLS["SENSEX"], "SENSEX"),
    ]
    cache_scan_results(results)
    return results


# =========================================================
# COMMANDS
# =========================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    await update.message.reply_text(
        "🚀 PRO TRADING BOT ACTIVE\n"
        "Commands:\n"
        "/scan\n"
        "/forcescan\n"
        "/summary\n"
        "/besttrade\n"
        "/why\n"
        "/performance\n"
        "/dailystats\n"
        "/notrade\n"
        "/adaptive\n"
        "/nifty\n"
        "/banknifty\n"
        "/sensex\n"
        "/signal\n"
        "/mode\n"
        "/settings\n"
        "/status\n"
        "/health\n"
        "/resetalerts\n"
        "/setthreshold 75\n"
        "/setcooldown 60\n"
        "/setalertmode on\n"
        "/setalertmode off"
    )


async def mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    await update.message.reply_text(
        f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
        f"Alert Threshold: {ALERT_THRESHOLD}%\n"
        f"Alert Cooldown: {ALERT_COOLDOWN_MINUTES} minutes"
    )


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    await update.message.reply_text(
        "⚙️ CURRENT SETTINGS\n\n"
        f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
        f"Alert Threshold: {ALERT_THRESHOLD}%\n"
        f"Alert Cooldown: {ALERT_COOLDOWN_MINUTES} minutes\n"
        f"State File: {ALERT_STATE_FILE}\n"
        f"Config File: {CONFIG_FILE}\n"
        f"Status File: {STATUS_FILE}\n"
        f"Trade Log File: {TRADE_LOG_FILE}"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %I:%M:%S %p IST")

    await update.message.reply_text(
        "🟢 BOT STATUS\n\n"
        f"Alive: Yes\n"
        f"Current Time: {now_ist}\n"
        f"Last Scan: {fmt_dt(LAST_SCAN_TIME)}\n"
        f"Last Alert: {fmt_dt(LAST_ALERT_TIME)}\n"
        f"Alerts Sent Today: {ALERTS_SENT_TODAY}\n"
        f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
        f"Alert Threshold: {ALERT_THRESHOLD}%\n"
        f"Alert Cooldown: {ALERT_COOLDOWN_MINUTES} minutes"
    )


async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    try:
        nifty_data = fetch_market_data(SYMBOLS["NIFTY 50"])
        bank_data = fetch_market_data(SYMBOLS["BANK NIFTY"])
        sensex_data = fetch_market_data(SYMBOLS["SENSEX"])

        nifty_age = round(get_data_age_minutes(nifty_data), 2) if not nifty_data.empty else -1
        bank_age = round(get_data_age_minutes(bank_data), 2) if not bank_data.empty else -1
        sensex_age = round(get_data_age_minutes(sensex_data), 2) if not sensex_data.empty else -1

        message = (
            "🩺 BOT HEALTH CHECK\n\n"
            f"Market Weekday: {is_market_weekday()}\n"
            f"Market Hours Now: {is_market_hours_now()}\n"
            f"Last Scan: {fmt_dt(LAST_SCAN_TIME)}\n"
            f"Last Alert: {fmt_dt(LAST_ALERT_TIME)}\n"
            f"Alerts Sent Today: {ALERTS_SENT_TODAY}\n\n"
            f"NIFTY Data Age: {nifty_age} min\n"
            f"BANKNIFTY Data Age: {bank_age} min\n"
            f"SENSEX Data Age: {sensex_age} min\n\n"
            f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
            f"Alert Threshold: {ALERT_THRESHOLD}%\n"
            f"Cooldown: {ALERT_COOLDOWN_MINUTES} min"
        )

        await update.message.reply_text(message)
    except Exception as e:
        await update.message.reply_text(f"❌ Health check failed: {e}")


async def resetalerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_ALERTS, ALERTS_SENT_TODAY, LAST_ALERT_TIME

    if not await require_authorized(update):
        return

    LAST_ALERTS = {}
    ALERTS_SENT_TODAY = 0
    LAST_ALERT_TIME = None

    save_alert_state()
    save_status()

    await update.message.reply_text("✅ Alert memory reset successfully")


async def setthreshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ALERT_THRESHOLD

    if not await require_authorized(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setthreshold 75")
        return

    try:
        value = int(context.args[0])

        if value < 0 or value > 100:
            await update.message.reply_text("Threshold must be between 0 and 100.")
            return

        ALERT_THRESHOLD = value
        save_config()
        await update.message.reply_text(f"✅ Alert threshold updated to {ALERT_THRESHOLD}%")
    except ValueError:
        await update.message.reply_text("Please enter a valid integer. Example: /setthreshold 75")


async def setcooldown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ALERT_COOLDOWN_MINUTES

    if not await require_authorized(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setcooldown 60")
        return

    try:
        value = int(context.args[0])

        if value < 1 or value > 1440:
            await update.message.reply_text("Cooldown must be between 1 and 1440 minutes.")
            return

        ALERT_COOLDOWN_MINUTES = value
        save_config()
        await update.message.reply_text(f"✅ Alert cooldown updated to {ALERT_COOLDOWN_MINUTES} minutes")
    except ValueError:
        await update.message.reply_text("Please enter a valid integer. Example: /setcooldown 60")


async def setalertmode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ALERT_ONLY_MODE

    if not await require_authorized(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setalertmode on  or  /setalertmode off")
        return

    value = context.args[0].strip().lower()

    if value == "on":
        ALERT_ONLY_MODE = True
        save_config()
        await update.message.reply_text("✅ Alert-only mode is now ON")
        return

    if value == "off":
        ALERT_ONLY_MODE = False
        save_config()
        await update.message.reply_text("✅ Alert-only mode is now OFF")
        return

    await update.message.reply_text("Usage: /setalertmode on  or  /setalertmode off")


async def signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    if LAST_SCAN_RESULTS:
        nifty_result = LAST_SCAN_RESULTS.get("NIFTY 50", analyze_market(SYMBOLS["NIFTY 50"], "NIFTY 50"))
        bank_result = LAST_SCAN_RESULTS.get("BANK NIFTY", analyze_market(SYMBOLS["BANK NIFTY"], "BANK NIFTY"))
        sensex_result = LAST_SCAN_RESULTS.get("SENSEX", analyze_market(SYMBOLS["SENSEX"], "SENSEX"))
    else:
        results = await run_full_scan_once()
        nifty_result, bank_result, sensex_result = results

    message = (
        "📌 QUICK SIGNAL SUMMARY\n\n"
        f"{build_signal_summary(nifty_result)}\n\n"
        f"{build_signal_summary(bank_result)}\n\n"
        f"{build_signal_summary(sensex_result)}"
    )
    await update.message.reply_text(message)


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    results = await run_full_scan_once()

    message = (
        "📊 MANUAL MARKET SCAN\n\n"
        f"{results[0]['message']}\n\n"
        f"{results[1]['message']}\n\n"
        f"{results[2]['message']}"
    )
    await update.message.reply_text(message)


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    await update.message.reply_text(LAST_SCAN_SUMMARY_TEXT)


async def besttrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    results = list(LAST_SCAN_RESULTS.values())
    if not results:
        results = await run_full_scan_once()

    best = get_best_signal_result(results)
    await update.message.reply_text(build_best_trade_message(best))


async def forcescan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_SCAN_TIME

    if not await require_authorized(update):
        return

    if SCAN_LOCK.locked():
        await update.message.reply_text("⏳ Scan already running")
        return

    async with SCAN_LOCK:
        try:
            LAST_SCAN_TIME = datetime.now(IST)
            save_status()

            results = await run_full_scan_once()

            message = (
                "⚡ FORCE SCAN COMPLETE\n\n"
                f"{results[0]['message']}\n\n"
                f"{results[1]['message']}\n\n"
                f"{results[2]['message']}"
            )
            await update.message.reply_text(message)

        except Exception as e:
            logger.exception("Force scan failed: %s", e)
            await update.message.reply_text(f"❌ Force scan failed: {e}")


async def why(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    if not LAST_SCAN_RESULTS:
        await update.message.reply_text("Run /scan first")
        return

    msg = ["🧠 WHY SIGNAL ANALYSIS\n"]

    for name, r in LAST_SCAN_RESULTS.items():
        explanation = []

        if r.get("signal") == "NONE":
            explanation.append("❌ No Trade")

        if r.get("trend") == r.get("htf_trend") and r.get("trend") != "Neutral":
            explanation.append("✅ Trend aligned with HTF")

        if safe_float(r.get("adx", 0), 0) >= 18:
            explanation.append("✅ Strong Trend (ADX)")
        else:
            explanation.append("⚠ ADX Weak")

        if r.get("signal") == "CALL" and safe_float(r.get("price", 0), 0) > safe_float(r.get("vwap", 0), 0):
            explanation.append("✅ Above VWAP")

        if r.get("signal") == "PUT" and safe_float(r.get("price", 0), 0) < safe_float(r.get("vwap", 0), 0):
            explanation.append("✅ Below VWAP")

        if r.get("volume_breakout"):
            explanation.append("✅ Volume Breakout")
        else:
            explanation.append("⚠ No Volume Breakout")

        if r.get("orb_break"):
            explanation.append(f"✅ ORB {r.get('orb_direction','NONE')}")

        if r.get("vwap_behavior") == "RECLAIM":
            explanation.append("✅ VWAP Reclaim")

        if r.get("vwap_behavior") == "REJECTION":
            explanation.append("✅ VWAP Rejection")

        if r.get("strong_candle"):
            explanation.append("✅ Strong Candle")
        else:
            explanation.append("⚠ Weak Candle")

        if r.get("regime") == "TREND DAY":
            explanation.append("🔥 Trend Day")

        if r.get("regime") == "BREAKOUT DAY":
            explanation.append("🔥 Breakout Day")

        if r.get("no_trade_day", False):
            explanation.append("🚫 No Trade Day")

        adaptive_notes = r.get("adaptive_notes", [])
        if adaptive_notes:
            explanation.append(f"⚙ Adaptive: {', '.join(adaptive_notes)}")

        msg.append(f"{name}")
        msg.append(" | ".join(explanation))
        msg.append("")

    await update.message.reply_text("\n".join(msg))


async def performance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    df = load_trade_log_df()
    if df.empty:
        await update.message.reply_text("No trades logged")
        return

    total = len(df)
    calls = len(df[df["signal"] == "CALL"])
    puts = len(df[df["signal"] == "PUT"])
    closed = len(df[df["status"] == "CLOSED"])
    targets = len(df[df["outcome"] == "TARGET_HIT"])
    stops = len(df[df["outcome"].isin(["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"])])
    expired = len(df[df["status"] == "EXPIRED"])

    rr_series = pd.to_numeric(df["rr"], errors="coerce")
    avg_conf = round(pd.to_numeric(df["confidence"], errors="coerce").mean(), 2) if total > 0 else 0.0
    avg_rr = round(rr_series.dropna().mean(), 2) if rr_series.notna().any() else 0.0
    win_rate = round((targets / closed) * 100, 2) if closed > 0 else 0.0

    msg = (
        "📊 PERFORMANCE STATS\n\n"
        f"Total Trades Logged: {total}\n"
        f"CALL Signals: {calls}\n"
        f"PUT Signals: {puts}\n"
        f"Closed Trades: {closed}\n"
        f"Target Hits: {targets}\n"
        f"Stop Loss Hits: {stops}\n"
        f"Expired Trades: {expired}\n"
        f"Win Rate: {win_rate}%\n"
        f"Avg Confidence: {avg_conf}%\n"
        f"Avg RR: {avg_rr}\n"
    )

    await update.message.reply_text(msg)


async def dailystats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    await update.message.reply_text(get_daily_stats_message())


async def notrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    results = list(LAST_SCAN_RESULTS.values())
    if not results:
        results = await run_full_scan_once()

    lines = ["🚫 NO TRADE ENGINE REPORT", ""]
    for r in results:
        reasons = r.get("no_trade_reasons", [])
        status = "YES" if r.get("no_trade_day", False) else "NO"
        lines.append(f"{r.get('name', 'UNKNOWN')}")
        lines.append(f"No Trade Day: {status}")
        lines.append(f"Reasons: {', '.join(reasons) if reasons else 'None'}")
        lines.append("")

    await update.message.reply_text("\n".join(lines))


async def adaptive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    adaptive_info = get_adaptive_settings()
    metrics = adaptive_info["metrics"]

    message = (
        "⚙ ADAPTIVE BEHAVIOR REPORT\n\n"
        f"Recent Trades Checked: {metrics['total']}\n"
        f"Closed Trades: {metrics['closed']}\n"
        f"Target Hits: {metrics['targets']}\n"
        f"Stop Losses: {metrics['stops']}\n"
        f"Win Rate: {metrics['win_rate']}%\n"
        f"Avg RR: {metrics['avg_rr']}\n"
        f"Consecutive Losses: {metrics['consecutive_losses']}\n\n"
        f"Threshold Boost: {adaptive_info['threshold_boost']}\n"
        f"Confidence Penalty: {adaptive_info['confidence_penalty']}\n"
        f"Hard Block: {adaptive_info['hard_block']}\n"
        f"Notes: {', '.join(adaptive_info['notes']) if adaptive_info['notes'] else 'None'}"
    )
    await update.message.reply_text(message)


async def nifty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    result = analyze_market(SYMBOLS["NIFTY 50"], "NIFTY 50")
    await update.message.reply_text(result["message"])


async def banknifty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    result = analyze_market(SYMBOLS["BANK NIFTY"], "BANK NIFTY")
    await update.message.reply_text(result["message"])


async def sensex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    result = analyze_market(SYMBOLS["SENSEX"], "SENSEX")
    await update.message.reply_text(result["message"])


# =========================================================
# AUTO SCAN
# =========================================================
async def auto_scan(context: ContextTypes.DEFAULT_TYPE):
    global LAST_SCAN_TIME, LAST_ALERT_TIME, ALERTS_SENT_TODAY

    if SCAN_LOCK.locked():
        logger.info("Skipping auto scan because previous scan is still running")
        return

    async with SCAN_LOCK:
        try:
            reset_daily_alert_counter_if_needed()
            cleanup_old_alerts()
            update_trade_outcomes()

            LAST_SCAN_TIME = datetime.now(IST)
            save_status()

            if ALERT_ONLY_MODE and (not is_market_open_day() or not is_active_market_session()):
                logger.info("Skipping alert-only auto scan outside active market session")
                return

            results = await run_full_scan_once()

            if ALERT_ONLY_MODE:
                for result in results:
                    if should_send_alert(result) and not is_duplicate_alert(result):
                        alert_message = (
                            "🚨 HIGH CONFIDENCE ALERT\n\n"
                            f"{result['message']}"
                        )
                        await context.bot.send_message(chat_id=CHAT_ID, text=alert_message)
                        LAST_ALERT_TIME = datetime.now(IST)
                        ALERTS_SENT_TODAY += 1
                        process_alert_logging(result)
                        save_status()
            else:
                message = (
                    "📊 MARKET SCAN\n\n"
                    f"{results[0]['message']}\n\n"
                    f"{results[1]['message']}\n\n"
                    f"{results[2]['message']}"
                )
                await context.bot.send_message(chat_id=CHAT_ID, text=message)

        except Exception as e:
            logger.exception("Auto scan failed: %s", e)
            try:
                await context.bot.send_message(chat_id=CHAT_ID, text=f"Error: {e}")
            except Exception:
                logger.exception("Failed to send error message to Telegram")


# =========================================================
# MAIN
# =========================================================
def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing. Set it in your environment variables.")

    if not CHAT_ID:
        raise ValueError("CHAT_ID is missing. Set it in your environment variables.")

    if AUTHORIZED_CHAT_ID == 0:
        raise ValueError("AUTHORIZED_CHAT_ID is missing or invalid. Set it as an integer environment variable.")

    load_config()
    load_alert_state()
    load_status()
    reset_daily_alert_counter_if_needed()
    init_trade_log()
    update_trade_outcomes()

    logger.info("Starting bot with DATA_DIR=%s", DATA_DIR)
    logger.info("Alert only mode=%s | threshold=%s | cooldown=%s", ALERT_ONLY_MODE, ALERT_THRESHOLD, ALERT_COOLDOWN_MINUTES)

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mode", mode))
    app.add_handler(CommandHandler("settings", settings))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("resetalerts", resetalerts))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("besttrade", besttrade))
    app.add_handler(CommandHandler("forcescan", forcescan))
    app.add_handler(CommandHandler("why", why))
    app.add_handler(CommandHandler("performance", performance))
    app.add_handler(CommandHandler("dailystats", dailystats))
    app.add_handler(CommandHandler("notrade", notrade))
    app.add_handler(CommandHandler("adaptive", adaptive))
    app.add_handler(CommandHandler("setthreshold", setthreshold))
    app.add_handler(CommandHandler("setcooldown", setcooldown))
    app.add_handler(CommandHandler("setalertmode", setalertmode))
    app.add_handler(CommandHandler("signal", signal))
    app.add_handler(CommandHandler("scan", scan))
    app.add_handler(CommandHandler("nifty", nifty))
    app.add_handler(CommandHandler("banknifty", banknifty))
    app.add_handler(CommandHandler("sensex", sensex))

    if app.job_queue is None:
        raise RuntimeError("JobQueue is not available. Install python-telegram-bot with job-queue support.")

    app.job_queue.run_repeating(auto_scan, interval=900, first=10)

    print("Bot running...")
    app.run_polling()


if __name__ == "__main__":
    main()

# =========================================================
# CONFIG
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
AUTHORIZED_CHAT_ID_RAW = os.getenv("AUTHORIZED_CHAT_ID", "").strip()

if AUTHORIZED_CHAT_ID_RAW:
    try:
        AUTHORIZED_CHAT_ID = int(AUTHORIZED_CHAT_ID_RAW)
    except ValueError as exc:
        raise ValueError("AUTHORIZED_CHAT_ID must be a valid integer.") from exc
else:
    AUTHORIZED_CHAT_ID = 0

IST = ZoneInfo("Asia/Kolkata")

DATA_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

ALERT_STATE_FILE = str(DATA_DIR / "alert_state.json")
CONFIG_FILE = str(DATA_DIR / "bot_config.json")
STATUS_FILE = str(DATA_DIR / "bot_status.json")
TRADE_LOG_FILE = str(DATA_DIR / "trade_log.csv")

DEFAULT_ALERT_ONLY_MODE = True
DEFAULT_ALERT_THRESHOLD = 75
DEFAULT_ALERT_COOLDOWN_MINUTES = 45

DEFAULT_INTERVAL = "15m"
DEFAULT_PERIOD = "5d"

HIGHER_TF_INTERVAL = "60m"
HIGHER_TF_PERIOD = "10d"

MIN_REQUIRED_BARS = 100
STALE_DATA_MAX_DELAY_MINUTES = 35
TRADE_MAX_AGE_DAYS = 2

SYMBOLS = {
    "NIFTY 50": "^NSEI",
    "BANK NIFTY": "^NSEBANK",
    "SENSEX": "^BSESN",
}

ALERT_ONLY_MODE = DEFAULT_ALERT_ONLY_MODE
ALERT_THRESHOLD = DEFAULT_ALERT_THRESHOLD
ALERT_COOLDOWN_MINUTES = DEFAULT_ALERT_COOLDOWN_MINUTES

LAST_ALERTS: Dict[str, datetime] = {}
LAST_SCAN_TIME: Optional[datetime] = None
LAST_ALERT_TIME: Optional[datetime] = None
ALERTS_SENT_TODAY = 0
ALERTS_SENT_DATE: Optional[str] = None

SCAN_LOCK = asyncio.Lock()
LAST_SCAN_RESULTS: Dict[str, dict] = {}
LAST_SCAN_SUMMARY_TEXT = "No scan yet"


# =========================================================
# AUTH
# =========================================================
def is_authorized(update: Update) -> bool:
    if update.effective_chat is None:
        return False
    return update.effective_chat.id == AUTHORIZED_CHAT_ID


async def require_authorized(update: Update) -> bool:
    if not is_authorized(update):
        if update.effective_message is not None:
            await update.effective_message.reply_text("Unauthorized user")
        return False
    return True


# =========================================================
# PERSISTENCE
# =========================================================
def load_config():
    global ALERT_ONLY_MODE, ALERT_THRESHOLD, ALERT_COOLDOWN_MINUTES

    if not os.path.exists(CONFIG_FILE):
        save_config()
        return

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)

        ALERT_ONLY_MODE = bool(config.get("alert_only_mode", DEFAULT_ALERT_ONLY_MODE))
        ALERT_THRESHOLD = int(config.get("alert_threshold", DEFAULT_ALERT_THRESHOLD))
        ALERT_COOLDOWN_MINUTES = int(
            config.get("alert_cooldown_minutes", DEFAULT_ALERT_COOLDOWN_MINUTES)
        )
    except Exception as e:
        logger.warning("Failed to load config: %s", e)
        ALERT_ONLY_MODE = DEFAULT_ALERT_ONLY_MODE
        ALERT_THRESHOLD = DEFAULT_ALERT_THRESHOLD
        ALERT_COOLDOWN_MINUTES = DEFAULT_ALERT_COOLDOWN_MINUTES


def save_config():
    config = {
        "alert_only_mode": ALERT_ONLY_MODE,
        "alert_threshold": ALERT_THRESHOLD,
        "alert_cooldown_minutes": ALERT_COOLDOWN_MINUTES,
    }

    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save config: %s", e)


def load_alert_state():
    global LAST_ALERTS

    if not os.path.exists(ALERT_STATE_FILE):
        LAST_ALERTS = {}
        return

    try:
        with open(ALERT_STATE_FILE, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        loaded = {}
        for key, value in raw_data.items():
            loaded[key] = datetime.fromisoformat(value)

        LAST_ALERTS = loaded
    except Exception as e:
        logger.warning("Failed to load alert state: %s", e)
        LAST_ALERTS = {}


def save_alert_state():
    try:
        serializable = {}
        for key, value in LAST_ALERTS.items():
            serializable[key] = value.isoformat()

        with open(ALERT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save alert state: %s", e)


def load_status():
    global LAST_SCAN_TIME, LAST_ALERT_TIME, ALERTS_SENT_TODAY, ALERTS_SENT_DATE

    if not os.path.exists(STATUS_FILE):
        save_status()
        return

    try:
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        LAST_SCAN_TIME = (
            datetime.fromisoformat(data["last_scan_time"])
            if data.get("last_scan_time")
            else None
        )
        LAST_ALERT_TIME = (
            datetime.fromisoformat(data["last_alert_time"])
            if data.get("last_alert_time")
            else None
        )
        ALERTS_SENT_TODAY = int(data.get("alerts_sent_today", 0))
        ALERTS_SENT_DATE = data.get("alerts_sent_date")
    except Exception as e:
        logger.warning("Failed to load status: %s", e)
        LAST_SCAN_TIME = None
        LAST_ALERT_TIME = None
        ALERTS_SENT_TODAY = 0
        ALERTS_SENT_DATE = None


def save_status():
    try:
        data = {
            "last_scan_time": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None,
            "last_alert_time": LAST_ALERT_TIME.isoformat() if LAST_ALERT_TIME else None,
            "alerts_sent_today": ALERTS_SENT_TODAY,
            "alerts_sent_date": ALERTS_SENT_DATE,
        }

        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save status: %s", e)


def reset_daily_alert_counter_if_needed():
    global ALERTS_SENT_TODAY, ALERTS_SENT_DATE

    today_str = datetime.now(IST).date().isoformat()
    if ALERTS_SENT_DATE != today_str:
        ALERTS_SENT_TODAY = 0
        ALERTS_SENT_DATE = today_str
        save_status()


def cleanup_old_alerts():
    now = datetime.now(IST)
    keys_to_delete = []

    for key, timestamp in LAST_ALERTS.items():
        if now - timestamp > timedelta(hours=24):
            keys_to_delete.append(key)

    for key in keys_to_delete:
        del LAST_ALERTS[key]

    if keys_to_delete:
        save_alert_state()


# =========================================================
# TRADE LOGGER
# =========================================================
TRADE_LOG_COLUMNS = [
    "trade_id",
    "signal_time",
    "symbol",
    "yahoo_symbol",
    "signal",
    "entry_mode",
    "trigger_price",
    "entry_price",
    "entry_time",
    "stop_loss",
    "target_price",
    "trailing_sl",
    "price_at_signal",
    "confidence",
    "regime",
    "trend",
    "htf_trend",
    "adx",
    "vwap",
    "volume_breakout",
    "status",
    "outcome",
    "exit_price",
    "exit_time",
    "rr",
    "notes",
]


def init_trade_log():
    if not Path(TRADE_LOG_FILE).exists():
        with open(TRADE_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(TRADE_LOG_COLUMNS)


def load_trade_log_df() -> pd.DataFrame:
    init_trade_log()
    try:
        df = pd.read_csv(TRADE_LOG_FILE)
        if df.empty:
            return pd.DataFrame(columns=TRADE_LOG_COLUMNS)
        for col in TRADE_LOG_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df
    except Exception as e:
        logger.warning("Failed to load trade log: %s", e)
        return pd.DataFrame(columns=TRADE_LOG_COLUMNS)


def save_trade_log_df(df: pd.DataFrame):
    try:
        df.to_csv(TRADE_LOG_FILE, index=False)
    except Exception as e:
        logger.warning("Failed to save trade log: %s", e)


def log_trade(result: dict):
    try:
        df = load_trade_log_df()

        trigger_price = safe_float(result.get("entry_trigger_price", result.get("price", 0)))
        entry_mode = result.get("entry", "No Trade")
        signal_time = datetime.now(IST)
        trade_id = f"{result.get('name','UNKNOWN')}-{signal_time.strftime('%Y%m%d%H%M%S')}"

        new_row = {
            "trade_id": trade_id,
            "signal_time": signal_time.isoformat(),
            "symbol": result.get("name"),
            "yahoo_symbol": SYMBOLS.get(result.get("name"), ""),
            "signal": result.get("signal"),
            "entry_mode": entry_mode,
            "trigger_price": trigger_price,
            "entry_price": "",
            "entry_time": "",
            "stop_loss": result.get("stop_loss", 0),
            "target_price": result.get("target_price", 0),
            "trailing_sl": result.get("trailing_sl", 0),
            "price_at_signal": result.get("price", 0),
            "confidence": result.get("confidence", 0),
            "regime": result.get("regime", "UNKNOWN"),
            "trend": result.get("trend", "Neutral"),
            "htf_trend": result.get("htf_trend", "Neutral"),
            "adx": result.get("adx", 0),
            "vwap": result.get("vwap", 0),
            "volume_breakout": result.get("volume_breakout", False),
            "status": "PENDING",
            "outcome": "",
            "exit_price": "",
            "exit_time": "",
            "rr": "",
            "notes": "",
        }

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_trade_log_df(df)

    except Exception as e:
        logger.warning("Trade log failed: %s", e)


# =========================================================
# HELPERS
# =========================================================
def safe_float(value, default=0.0) -> float:
    try:
        if pd.isna(value):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def fmt_dt(dt_obj):
    if dt_obj is None:
        return "Never"
    return dt_obj.astimezone(IST).strftime("%Y-%m-%d %I:%M:%S %p IST")


def extract_series(data: pd.DataFrame, column_name: str) -> pd.Series:
    col = data[column_name]

    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]

    col = pd.Series(col).copy()
    col = pd.to_numeric(col, errors="coerce")
    col = col.dropna()
    return col


def normalize_ohlc_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame()

    df = data.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    required = ["Open", "High", "Low", "Close"]
    for col in required:
        if col not in df.columns:
            return pd.DataFrame()

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    return df


def is_market_weekday() -> bool:
    return datetime.now(IST).weekday() < 5


def is_market_hours_now() -> bool:
    now = datetime.now(IST).time()
    return dt_time(9, 15) <= now <= dt_time(15, 30)


def is_market_open_day() -> bool:
    return is_market_weekday()


def is_active_market_session() -> bool:
    return is_market_hours_now()


def get_data_age_minutes(data: pd.DataFrame) -> float:
    try:
        if data is None or data.empty:
            return 999999.0

        last_idx = data.index[-1]

        if hasattr(last_idx, "to_pydatetime"):
            last_ts = last_idx.to_pydatetime()
        else:
            last_ts = pd.Timestamp(last_idx).to_pydatetime()

        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=IST)
        else:
            last_ts = last_ts.astimezone(IST)

        now = datetime.now(IST)
        diff = now - last_ts
        return max(diff.total_seconds() / 60.0, 0.0)
    except Exception:
        return 999999.0


def is_data_stale(data: pd.DataFrame, max_delay_minutes: int = STALE_DATA_MAX_DELAY_MINUTES) -> bool:
    if not is_market_weekday():
        return False
    if not is_market_hours_now():
        return False
    return get_data_age_minutes(data) > max_delay_minutes


def build_compact_result_line(result: dict) -> str:
    return (
        f"{result.get('name', 'UNKNOWN')} | "
        f"Signal: {result.get('signal', 'NONE')} | "
        f"Confidence: {result.get('confidence', 0)}% | "
        f"Strength: {result.get('strength', '❌ WEAK')} | "
        f"Entry: {result.get('entry', 'No Trade')}"
    )


def build_scan_summary(results: list) -> str:
    lines = ["📋 LAST SCAN SUMMARY", ""]
    for result in results:
        lines.append(build_compact_result_line(result))
    return "\n".join(lines)


def cache_scan_results(results: list):
    global LAST_SCAN_RESULTS, LAST_SCAN_SUMMARY_TEXT
    LAST_SCAN_RESULTS = {result["name"]: result for result in results}
    LAST_SCAN_SUMMARY_TEXT = build_scan_summary(results)
_original_cache_scan_results = cache_scan_results

def cache_scan_results(results: list):
    print("🚀 AGGRESSIVE MODE ACTIVE")

    upgraded = []
    for r in results:
        upgraded.append(aggressive_signal_boost(r))

    _original_cache_scan_results(upgraded)

def get_best_signal_result(results: list) -> dict:
    valid = [
        r for r in results
        if r.get("signal") != "NONE"
        and r.get("expected_move") not in [
            "Market Closed",
            "Avoid / Low Probability",
            "Avoid This Session",
            "Unknown",
            "Blocked - Stale Data",
            "Avoid / No Trade Day",
            "Adaptive Risk Lock",
            "Adaptive Filtered",
        ]
    ]

    if not valid:
        return {}

    valid.sort(
        key=lambda x: (
            x.get("confidence", 0),
            1 if x.get("entry") == "Enter Now" else 0,
            x.get("price", 0),
        ),
        reverse=True,
    )
    return valid[0]


def build_best_trade_message(result: dict) -> str:
    if not result:
        return "❌ No valid trade setup found in last scan"

    msg = [
        "🏆 BEST TRADE SETUP",
        "",
        f"Index: {result.get('name', 'UNKNOWN')}",
        f"Signal: {result.get('signal', 'NONE')}",
        f"Confidence: {result.get('confidence', 0)}%",
        f"Strength: {result.get('strength', '❌ WEAK')}",
        f"Entry: {result.get('entry', 'No Trade')}",
        f"Trigger Price: {result.get('entry_trigger_price', 0)}",
        f"Trend: {result.get('trend', 'Neutral')}",
        f"Higher TF Trend: {result.get('htf_trend', 'Neutral')}",
        f"Regime: {result.get('regime', 'UNKNOWN')}",
        f"Price: {result.get('price', 0)}",
        f"SL: {result.get('stop_loss', 0)}",
        f"Target: {result.get('target_price', 0)}",
        f"Trailing SL: {result.get('trailing_sl', 0)}",
        f"Expected Move: {result.get('expected_move', 'Unknown')}",
        f"ADX: {result.get('adx', 0)}",
        f"VWAP: {result.get('vwap', 0)}",
    ]

    strikes = result.get("strikes", {})
    if strikes:
        msg.extend([
            "",
            "🎯 Option Strikes:",
            f"ATM: {strikes.get('ATM', '-')}",
            f"ITM: {strikes.get('ITM', '-')}",
            f"OTM: {strikes.get('OTM', '-')}",
        ])

    return "\n".join(msg)


# =========================================================
# MARKET LOGIC
# =========================================================
def market_bias_proxy(close: pd.Series, ema20: pd.Series, ema50: pd.Series) -> Tuple[str, float]:
    if len(close) < 5 or len(ema20) < 1 or len(ema50) < 1:
        return "Neutral", 1.0

    price = safe_float(close.iloc[-1])
    prev_close = safe_float(close.iloc[-2], price)
    ema20_last = safe_float(ema20.iloc[-1])
    ema50_last = safe_float(ema50.iloc[-1])

    bullish = 0
    bearish = 0

    if price > ema20_last:
        bullish += 1
    elif price < ema20_last:
        bearish += 1

    if ema20_last > ema50_last:
        bullish += 1
    elif ema20_last < ema50_last:
        bearish += 1

    if price > prev_close:
        bullish += 1
    elif price < prev_close:
        bearish += 1

    if bullish >= 2 and bullish > bearish:
        return "Bullish", 0.90
    if bearish >= 2 and bearish > bullish:
        return "Bearish", 1.10
    return "Neutral", 1.00


def liquidity_map(high: pd.Series, low: pd.Series, close: pd.Series):
    if len(high) < 96 or len(low) < 96 or len(close) < 1:
        return "Not enough data", 0.0

    prev_day_high = safe_float(high.iloc[-96:-48].max())
    prev_day_low = safe_float(low.iloc[-96:-48].min())
    price = safe_float(close.iloc[-1])

    dist_high = abs(prev_day_high - price)
    dist_low = abs(price - prev_day_low)

    if dist_high < dist_low:
        return "Previous Day High", round(dist_high, 2)
    return "Previous Day Low", round(dist_low, 2)


def get_session_info():
    now = datetime.now(IST)
    current_time = now.time()

    market_open = dt_time(9, 15)
    market_close = dt_time(15, 30)

    if current_time < market_open or current_time > market_close:
        return "MARKET CLOSED", -50, False

    if dt_time(9, 15) <= current_time < dt_time(9, 45):
        return "OPENING VOLATILITY", 5, True

    if dt_time(9, 45) <= current_time < dt_time(11, 30):
        return "PRIME TREND WINDOW", 20, True

    if dt_time(11, 30) <= current_time < dt_time(13, 15):
        return "MIDDAY CHOP", -20, False

    if dt_time(13, 15) <= current_time < dt_time(14, 30):
        return "AFTERNOON BUILDUP", 10, True

    if dt_time(14, 30) <= current_time <= dt_time(15, 30):
        return "CLOSING MOVE WINDOW", 20, True

    return "UNKNOWN SESSION", 0, False


def calculate_atr_proxy(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(length, min_periods=length).mean()
    return atr


def calculate_adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    try:
        return ADXIndicator(high=high, low=low, close=close, window=length).adx()
    except Exception:
        return pd.Series([0.0] * len(close), index=close.index)


def calculate_vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    try:
        typical_price = (high + low + close) / 3
        volume_safe = volume.replace(0, pd.NA).ffill().fillna(1)
        return (typical_price * volume_safe).cumsum() / volume_safe.cumsum()
    except Exception:
        return pd.Series([0.0] * len(close), index=close.index)


def is_volume_breakout(volume: pd.Series) -> bool:
    try:
        if len(volume) < 20:
            return False
        recent_vol = safe_float(volume.iloc[-1], 0)
        avg_vol = safe_float(volume.iloc[-20:-1].mean(), 0)
        if avg_vol <= 0:
            return False
        return recent_vol > avg_vol * 1.5
    except Exception:
        return False


def get_opening_range(data: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
    try:
        if data is None or data.empty or len(data) < 3:
            return None, None

        or_high = safe_float(data["High"].iloc[:3].max(), 0)
        or_low = safe_float(data["Low"].iloc[:3].min(), 0)
        return or_high, or_low
    except Exception:
        return None, None


def check_orb_breakout(price: float, or_high: Optional[float], or_low: Optional[float]) -> Tuple[bool, str]:
    if or_high is None or or_low is None:
        return False, "NONE"

    if price > or_high:
        return True, "CALL"
    if price < or_low:
        return True, "PUT"
    return False, "NONE"


def vwap_behavior(close: pd.Series, vwap: pd.Series) -> str:
    try:
        if len(close) < 3 or len(vwap) < 3:
            return "NEUTRAL"

        c1 = safe_float(close.iloc[-2])
        c2 = safe_float(close.iloc[-1])
        v1 = safe_float(vwap.iloc[-2])
        v2 = safe_float(vwap.iloc[-1])

        if c1 < v1 and c2 > v2:
            return "RECLAIM"

        if c1 > v1 and c2 < v2:
            return "REJECTION"

        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


def is_strong_candle(open_price: float, high_price: float, low_price: float, close_price: float) -> bool:
    try:
        body = abs(close_price - open_price)
        candle_range = high_price - low_price

        if candle_range <= 0:
            return False

        body_ratio = body / candle_range
        upper_wick = high_price - max(open_price, close_price)
        lower_wick = min(open_price, close_price) - low_price

        if body_ratio > 0.6 and upper_wick < body and lower_wick < body:
            return True

        return False
    except Exception:
        return False


def detect_breakout_confirmation(close: pd.Series, high: pd.Series, low: pd.Series) -> Tuple[bool, str]:
    if len(close) < 22 or len(high) < 22 or len(low) < 22:
        return False, "NONE"

    price = safe_float(close.iloc[-1])
    recent_high = safe_float(high.iloc[-21:-1].max())
    recent_low = safe_float(low.iloc[-21:-1].min())

    if price > recent_high:
        return True, "CALL"
    if price < recent_low:
        return True, "PUT"
    return False, "NONE"


def detect_momentum_alignment(trend: str, momentum: float) -> bool:
    if trend == "Bullish" and momentum >= 52:
        return True
    if trend == "Bearish" and momentum <= 48:
        return True
    return False


def get_higher_timeframe_trend(symbol: str) -> str:
    try:
        data = yf.download(
            symbol,
            interval=HIGHER_TF_INTERVAL,
            period=HIGHER_TF_PERIOD,
            auto_adjust=False,
            progress=False,
            threads=False,
        )

        data = normalize_ohlc_dataframe(data)
        if data.empty:
            return "Neutral"

        close = extract_series(data, "Close")
        if len(close) < 50:
            return "Neutral"

        ema20 = EMAIndicator(close, window=20).ema_indicator()
        ema50 = EMAIndicator(close, window=50).ema_indicator()

        ema20_last = safe_float(ema20.iloc[-1])
        ema50_last = safe_float(ema50.iloc[-1])

        if ema20_last > ema50_last:
            return "Bullish"
        if ema20_last < ema50_last:
            return "Bearish"
        return "Neutral"
    except Exception:
        return "Neutral"


def detect_entry_candle(close: pd.Series, open_: pd.Series, high: pd.Series, low: pd.Series) -> str:
    if len(close) < 3 or len(open_) < 3:
        return "NONE"

    last_close = safe_float(close.iloc[-1])
    last_open = safe_float(open_.iloc[-1])
    prev_close = safe_float(close.iloc[-2])
    prev_open = safe_float(open_.iloc[-2])

    if last_close > last_open and prev_close < prev_open and last_close > prev_open:
        return "CALL"

    if last_close < last_open and prev_close > prev_open and last_close < prev_open:
        return "PUT"

    return "NONE"


def get_option_strike(price: float, symbol_name: str, signal: str) -> dict:
    try:
        step = 100 if "BANK" in symbol_name.upper() else 50
        atm = round(price / step) * step

        if signal == "CALL":
            return {"ATM": int(atm), "ITM": int(atm - step), "OTM": int(atm + step)}

        if signal == "PUT":
            return {"ATM": int(atm), "ITM": int(atm + step), "OTM": int(atm - step)}

        return {}
    except Exception:
        return {}


def get_entry_timing_and_trigger(signal: str, close: pd.Series, high: pd.Series, low: pd.Series) -> Tuple[str, float]:
    try:
        if len(close) < 2 or len(high) < 2 or len(low) < 2:
            return "Wait", safe_float(close.iloc[-1], 0)

        last_close = safe_float(close.iloc[-1])
        prev_high = safe_float(high.iloc[-2])
        prev_low = safe_float(low.iloc[-2])

        if signal == "CALL":
            if last_close > prev_high:
                return "Enter Now", last_close
            return "Wait Breakout", prev_high

        if signal == "PUT":
            if last_close < prev_low:
                return "Enter Now", last_close
            return "Wait Breakdown", prev_low

        return "No Trade", last_close
    except Exception:
        return "Wait", 0.0


def classify_signal_strength(confidence: int, regime: str) -> str:
    if confidence >= 85:
        return "🔥 EXTREME"
    if confidence >= 75:
        return "💪 STRONG"
    if confidence >= 60:
        return "⚠ MODERATE"
    return "❌ WEAK"


def apply_trailing_sl(signal: str, price: float, atr_value: float) -> float:
    try:
        trail = max(atr_value * 0.5, 30)

        if signal == "CALL":
            return round(price - trail, 2)

        if signal == "PUT":
            return round(price + trail, 2)

        return 0.0
    except Exception:
        return 0.0


def detect_regime(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    ema20: pd.Series,
    ema50: pd.Series,
    momentum: float,
    compression: bool,
    vol_expansion: bool,
    liquidity_trap: bool,
):
    if len(close) < 30 or len(high) < 30 or len(low) < 30:
        return "UNKNOWN", 0

    recent_high = safe_float(high.iloc[-20:].max())
    recent_low = safe_float(low.iloc[-20:].min())
    price = safe_float(close.iloc[-1])

    day_range = recent_high - recent_low
    avg_recent_bar_range = safe_float((high.iloc[-20:] - low.iloc[-20:]).mean())
    ema_gap = abs(safe_float(ema20.iloc[-1]) - safe_float(ema50.iloc[-1]))

    breakout_up = price >= recent_high * 0.999
    breakout_down = price <= recent_low * 1.001

    regime = "RANGE DAY"
    regime_score = 0

    if compression and vol_expansion and (breakout_up or breakout_down):
        regime = "BREAKOUT DAY"
        regime_score = 25
    elif ema_gap > avg_recent_bar_range * 0.6 and (momentum > 60 or momentum < 40):
        regime = "TREND DAY"
        regime_score = 25
    elif liquidity_trap:
        regime = "REVERSAL DAY"
        regime_score = 10
    elif day_range < avg_recent_bar_range * 8:
        regime = "RANGE DAY"
        regime_score = -10

    return regime, regime_score


def calculate_expected_move(
    confidence: int,
    regime: str,
    session_name: str,
    trading_allowed: bool,
    atr_value: float,
) -> str:
    if session_name == "MARKET CLOSED":
        return "Market Closed"
    if regime == "RANGE DAY":
        return "Avoid / Low Probability"
    if not trading_allowed:
        return "Avoid This Session"

    if atr_value >= 220 or confidence >= 88:
        return "300-500 points"
    if atr_value >= 130 or confidence >= 75:
        return "150-300 points"
    if atr_value >= 70 or confidence >= 60:
        return "80-150 points"
    return "Small"


def generate_signal(
    symbol: str,
    trend: str,
    momentum: float,
    regime: str,
    trading_allowed: bool,
    liquidity_trap: bool,
    trap_direction: str,
    session_name: str,
    breakout_confirmed: bool,
    breakout_direction: str,
    close: pd.Series,
    open_: pd.Series,
    high: pd.Series,
    low: pd.Series,
) -> str:
    if session_name == "MARKET CLOSED":
        return "NONE"

    if regime not in ["TREND DAY", "BREAKOUT DAY"]:
        return "NONE"

    if not trading_allowed:
        return "NONE"

    if not detect_momentum_alignment(trend, momentum):
        return "NONE"

    htf_trend = get_higher_timeframe_trend(symbol)
    if htf_trend != "Neutral" and trend != htf_trend:
        return "NONE"

    entry_signal = detect_entry_candle(close, open_, high, low)

    if regime == "BREAKOUT DAY":
        if breakout_confirmed and breakout_direction in ["CALL", "PUT"] and breakout_direction == entry_signal:
            return breakout_direction
        return "NONE"

    if regime == "TREND DAY":
        if liquidity_trap and trap_direction != "NONE":
            return "NONE"
        if trend == "Bullish" and momentum > 55 and entry_signal == "CALL":
            return "CALL"
        if trend == "Bearish" and momentum < 45 and entry_signal == "PUT":
            return "PUT"

    return "NONE"


def apply_top3_filters(
    signal: str,
    price: float,
    adx_value: float,
    vwap_value: float,
    volume_breakout: bool,
    orb_break: bool,
    orb_dir: str,
    vwap_state: str,
    strong_candle: bool,
) -> Tuple[str, int, list]:
    reasons = []
    confidence_adjustment = 0

    if signal == "NONE":
        return signal, confidence_adjustment, reasons

    if adx_value < 18:
        reasons.append("Weak Trend (ADX Low)")

    if signal == "CALL" and price < vwap_value:
        reasons.append("Below VWAP")

    if signal == "PUT" and price > vwap_value:
        reasons.append("Above VWAP")

    if not volume_breakout:
        reasons.append("No Volume Breakout")

    if orb_break and signal == orb_dir:
        confidence_adjustment += 10
        reasons.append("ORB Breakout")

    if signal == "CALL":
        if vwap_state == "RECLAIM":
            confidence_adjustment += 10
            reasons.append("VWAP Reclaim")
        else:
            reasons.append("Bad VWAP Behavior")

    if signal == "PUT":
        if vwap_state == "REJECTION":
            confidence_adjustment += 10
            reasons.append("VWAP Rejection")
        else:
            reasons.append("Bad VWAP Behavior")

    if not strong_candle:
        reasons.append("Weak Candle")

    blocking_reasons = [
        r for r in reasons
        if r in [
            "Weak Trend (ADX Low)",
            "Below VWAP",
            "Above VWAP",
            "No Volume Breakout",
            "Bad VWAP Behavior",
            "Weak Candle",
        ]
    ]

    if blocking_reasons:
        return "NONE", confidence_adjustment - 15, reasons

    return signal, confidence_adjustment + 10, reasons


def detect_no_trade_conditions(result: dict) -> Tuple[bool, list]:
    reasons = []

    atr_value = safe_float(result.get("atr", 0), 0)
    adx_value = safe_float(result.get("adx", 0), 0)
    confidence = safe_float(result.get("confidence", 0), 0)
    regime = str(result.get("regime", "UNKNOWN"))
    session_name = str(result.get("session", "UNKNOWN"))
    vwap_state = str(result.get("vwap_behavior", "NEUTRAL"))
    volume_breakout = bool(result.get("volume_breakout", False))
    strong_candle = bool(result.get("strong_candle", False))
    orb_break = bool(result.get("orb_break", False))
    trend = str(result.get("trend", "Neutral"))
    htf_trend = str(result.get("htf_trend", "Neutral"))

    if session_name in ["MARKET CLOSED", "MIDDAY CHOP", "STALE DATA"]:
        reasons.append(f"Bad Session: {session_name}")

    if regime == "RANGE DAY":
        reasons.append("Range Day")

    if atr_value < 45:
        reasons.append("ATR Too Low")

    if adx_value < 16:
        reasons.append("ADX Too Weak")

    if confidence < max(ALERT_THRESHOLD - 10, 55):
        reasons.append("Confidence Too Low")

    if vwap_state == "NEUTRAL":
        reasons.append("VWAP Sideways")

    if not volume_breakout:
        reasons.append("No Volume Expansion")

    if not strong_candle:
        reasons.append("Weak Candle")

    if trend != "Neutral" and htf_trend != "Neutral" and trend != htf_trend:
        reasons.append("Trend Misaligned With HTF")

    if regime == "BREAKOUT DAY" and not orb_break:
        reasons.append("No ORB Breakout")

    no_trade = len(reasons) >= 3
    return no_trade, reasons


def get_recent_trade_metrics(lookback: int = 20) -> dict:
    df = load_trade_log_df()
    if df.empty:
        return {
            "total": 0,
            "closed": 0,
            "targets": 0,
            "stops": 0,
            "win_rate": 0.0,
            "avg_rr": 0.0,
            "consecutive_losses": 0,
        }

    recent = df.tail(lookback).copy()
    recent["rr"] = pd.to_numeric(recent["rr"], errors="coerce")

    closed = recent[recent["status"] == "CLOSED"].copy()
    targets = len(closed[closed["outcome"] == "TARGET_HIT"])
    stops = len(closed[closed["outcome"].isin(["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"])])
    win_rate = round((targets / len(closed)) * 100, 2) if len(closed) > 0 else 0.0
    avg_rr = round(closed["rr"].dropna().mean(), 2) if closed["rr"].notna().any() else 0.0

    consecutive_losses = 0
    for _, row in closed.iloc[::-1].iterrows():
        if row["outcome"] in ["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"]:
            consecutive_losses += 1
        else:
            break

    return {
        "total": len(recent),
        "closed": len(closed),
        "targets": targets,
        "stops": stops,
        "win_rate": win_rate,
        "avg_rr": avg_rr,
        "consecutive_losses": consecutive_losses,
    }


def get_adaptive_settings() -> dict:
    metrics = get_recent_trade_metrics(lookback=20)

    threshold_boost = 0
    confidence_penalty = 0
    hard_block = False
    notes = []

    if metrics["consecutive_losses"] >= 3:
        hard_block = True
        notes.append("3 Consecutive Losses")

    if metrics["closed"] >= 5 and metrics["win_rate"] < 35:
        threshold_boost += 10
        confidence_penalty += 10
        notes.append("Low Recent Win Rate")

    if metrics["avg_rr"] < 0 and metrics["closed"] >= 5:
        threshold_boost += 5
        confidence_penalty += 5
        notes.append("Negative Recent RR")

    if metrics["closed"] >= 5 and metrics["win_rate"] >= 60:
        threshold_boost -= 5
        notes.append("Good Recent Win Rate")

    return {
        "metrics": metrics,
        "threshold_boost": threshold_boost,
        "confidence_penalty": confidence_penalty,
        "hard_block": hard_block,
        "notes": notes,
    }


def calculate_sl_tp(signal: str, price: float, atr_value: float) -> Tuple[float, float]:
    atr_buffer = max(atr_value * 0.8, 40)

    if signal == "CALL":
        return round(price - atr_buffer, 2), round(price + atr_buffer * 1.8, 2)

    if signal == "PUT":
        return round(price + atr_buffer, 2), round(price - atr_buffer * 1.8, 2)

    return 0.0, 0.0


# =========================================================
# DATA FETCH + ANALYSIS
# =========================================================
def fetch_market_data(symbol: str) -> pd.DataFrame:
    try:
        data = yf.download(
            symbol,
            interval=DEFAULT_INTERVAL,
            period=DEFAULT_PERIOD,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return normalize_ohlc_dataframe(data)
    except Exception as e:
        logger.exception("Failed to fetch data for %s: %s", symbol, e)
        return pd.DataFrame()


def analyze_market(symbol: str, name: str) -> dict:
    data = fetch_market_data(symbol)

    default_result = {
        "name": name,
        "message": f"{name}\n\nData unavailable",
        "signal": "NONE",
        "confidence": 0,
        "expected_move": "Unknown",
        "regime": "UNKNOWN",
        "session": "UNKNOWN",
        "price": 0,
        "trend": "Neutral",
        "rsi": 0,
        "ema20": 0,
        "ema50": 0,
        "compression": False,
        "vol_expansion": False,
        "liquidity_trap": False,
        "options_bias": "Neutral",
        "pcr": 1.0,
        "liquidity_target": "Unknown",
        "distance": 0,
        "atr": 0,
        "adx": 0,
        "vwap": 0,
        "volume_breakout": False,
        "breakout_confirmed": False,
        "breakout_direction": "NONE",
        "orb_break": False,
        "orb_direction": "NONE",
        "vwap_behavior": "NEUTRAL",
        "strong_candle": False,
        "stop_loss": 0,
        "target_price": 0,
        "strength": "❌ WEAK",
        "entry": "No Trade",
        "entry_trigger_price": 0,
        "strikes": {},
        "trailing_sl": 0,
        "htf_trend": "Neutral",
        "no_trade_day": False,
        "no_trade_reasons": [],
        "adaptive_notes": [],
        "adaptive_threshold": ALERT_THRESHOLD,
    }

    if data.empty:
        return default_result

    if is_data_stale(data):
        stale_age = round(get_data_age_minutes(data), 2)
        default_result["message"] = (
            f"{name}\n\n"
            f"Data appears stale during market hours\n"
            f"Last candle age: {stale_age} minutes\n"
            f"Signal blocked for safety"
        )
        default_result["expected_move"] = "Blocked - Stale Data"
        default_result["session"] = "STALE DATA"
        return default_result

    close = extract_series(data, "Close")
    open_ = extract_series(data, "Open")
    high = extract_series(data, "High")
    low = extract_series(data, "Low")

    if "Volume" in data.columns:
        volume = extract_series(data, "Volume")
    else:
        volume = pd.Series([0.0] * len(close), index=close.index)

    min_len = min(len(close), len(open_), len(high), len(low), len(volume))
    close = close.iloc[-min_len:]
    open_ = open_.iloc[-min_len:]
    high = high.iloc[-min_len:]
    low = low.iloc[-min_len:]
    volume = volume.iloc[-min_len:]

    if min_len < MIN_REQUIRED_BARS:
        default_result["message"] = f"{name}\n\nNot enough data for analysis"
        return default_result

    rsi = RSIIndicator(close, window=14).rsi()
    ema20 = EMAIndicator(close, window=20).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    atr_series = calculate_atr_proxy(high, low, close, length=14)
    adx_series = calculate_adx(high, low, close, length=14)
    vwap_series = calculate_vwap(high, low, close, volume)

    price = safe_float(close.iloc[-1])
    ema20_last = safe_float(ema20.iloc[-1])
    ema50_last = safe_float(ema50.iloc[-1])
    momentum = safe_float(rsi.iloc[-1], 50)
    atr_value = safe_float(atr_series.iloc[-1], 0)
    adx_value = safe_float(adx_series.iloc[-1], 0)
    vwap_value = safe_float(vwap_series.iloc[-1], 0)
    volume_breakout = is_volume_breakout(volume)

    trend = "Neutral"
    trend_score = 0
    if ema20_last > ema50_last:
        trend = "Bullish"
        trend_score = 25
    elif ema20_last < ema50_last:
        trend = "Bearish"
        trend_score = 25

    momentum_score = 0
    if trend == "Bullish" and momentum >= 52:
        momentum_score = 20
    elif trend == "Bearish" and momentum <= 48:
        momentum_score = 20

    recent_range = safe_float((high.iloc[-5:] - low.iloc[-5:]).mean())
    past_range = safe_float((high.iloc[-20:-5] - low.iloc[-20:-5]).mean())
    compression = recent_range < (past_range * 0.6) if past_range > 0 else False
    compression_score = 12 if compression else 0

    vol_recent = safe_float((high.iloc[-3:] - low.iloc[-3:]).mean())
    vol_prev = safe_float((high.iloc[-10:-3] - low.iloc[-10:-3]).mean())
    vol_expansion = vol_recent > (vol_prev * 1.4) if vol_prev > 0 else False
    vol_score = 18 if vol_expansion else 0

    last_high = safe_float(high.iloc[-1])
    last_low = safe_float(low.iloc[-1])
    prev_high = safe_float(high.iloc[-10:-1].max())
    prev_low = safe_float(low.iloc[-10:-1].min())

    liquidity_trap = False
    trap_direction = "NONE"
    trap_score = 0

    if last_high > prev_high and price < prev_high:
        liquidity_trap = True
        trap_direction = "PUT"
        trap_score = 8
    elif last_low < prev_low and price > prev_low:
        liquidity_trap = True
        trap_direction = "CALL"
        trap_score = 8

    options_bias, pcr = market_bias_proxy(close, ema20, ema50)
    option_score = 0
    if options_bias == trend and trend != "Neutral":
        option_score = 10
    elif options_bias != "Neutral":
        option_score = 3

    target, distance = liquidity_map(high, low, close)
    liquidity_score = 15 if distance < 200 else 0

    breakout_confirmed, breakout_direction = detect_breakout_confirmation(close, high, low)
    breakout_score = 12 if breakout_confirmed else 0

    or_high, or_low = get_opening_range(data)
    orb_break, orb_direction = check_orb_breakout(price, or_high, or_low)
    vwap_state = vwap_behavior(close, vwap_series)
    strong_candle = is_strong_candle(
        safe_float(open_.iloc[-1]),
        safe_float(high.iloc[-1]),
        safe_float(low.iloc[-1]),
        safe_float(close.iloc[-1]),
    )

    regime, regime_score = detect_regime(
        close=close,
        high=high,
        low=low,
        ema20=ema20,
        ema50=ema50,
        momentum=momentum,
        compression=compression,
        vol_expansion=vol_expansion,
        liquidity_trap=liquidity_trap,
    )

    session_name, session_score, trading_allowed = get_session_info()
    htf_trend = get_higher_timeframe_trend(symbol)

    confidence = (
        trend_score
        + momentum_score
        + compression_score
        + vol_score
        + trap_score
        + option_score
        + liquidity_score
        + breakout_score
        + regime_score
        + session_score
    )
    confidence = max(0, min(confidence, 100))

    signal = generate_signal(
        symbol=symbol,
        trend=trend,
        momentum=momentum,
        regime=regime,
        trading_allowed=trading_allowed,
        liquidity_trap=liquidity_trap,
        trap_direction=trap_direction,
        session_name=session_name,
        breakout_confirmed=breakout_confirmed,
        breakout_direction=breakout_direction,
        close=close,
        open_=open_,
        high=high,
        low=low,
    )

    signal, confidence_adjustment, filter_notes = apply_top3_filters(
        signal=signal,
        price=price,
        adx_value=adx_value,
        vwap_value=vwap_value,
        volume_breakout=volume_breakout,
        orb_break=orb_break,
        orb_dir=orb_direction,
        vwap_state=vwap_state,
        strong_candle=strong_candle,
    )
    confidence = max(0, min(100, confidence + confidence_adjustment))

    expected_move = calculate_expected_move(
        confidence=confidence,
        regime=regime,
        session_name=session_name,
        trading_allowed=trading_allowed,
        atr_value=atr_value,
    )

    if expected_move in ["Market Closed", "Avoid / Low Probability", "Avoid This Session"]:
        signal = "NONE"

    stop_loss, target_price = calculate_sl_tp(signal, price, atr_value)
    strength = classify_signal_strength(confidence, regime)
    entry, entry_trigger_price = get_entry_timing_and_trigger(signal, close, high, low)
    strikes = get_option_strike(price, name, signal)
    trailing_sl = apply_trailing_sl(signal, price, atr_value)
    ultra_confirmation = "YES" if signal != "NONE" else "NO"

    result = {
        "name": name,
        "message": "",
        "signal": signal,
        "confidence": int(confidence),
        "expected_move": expected_move,
        "regime": regime,
        "session": session_name,
        "price": round(price, 2),
        "trend": trend,
        "rsi": round(momentum, 2),
        "ema20": round(ema20_last, 2),
        "ema50": round(ema50_last, 2),
        "compression": compression,
        "vol_expansion": vol_expansion,
        "liquidity_trap": liquidity_trap,
        "options_bias": options_bias,
        "pcr": pcr,
        "liquidity_target": target,
        "distance": distance,
        "atr": round(atr_value, 2),
        "adx": round(adx_value, 2),
        "vwap": round(vwap_value, 2),
        "volume_breakout": volume_breakout,
        "breakout_confirmed": breakout_confirmed,
        "breakout_direction": breakout_direction,
        "orb_break": orb_break,
        "orb_direction": orb_direction,
        "vwap_behavior": vwap_state,
        "strong_candle": strong_candle,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "strength": strength,
        "entry": entry,
        "entry_trigger_price": round(entry_trigger_price, 2),
        "strikes": strikes,
        "trailing_sl": trailing_sl,
        "htf_trend": htf_trend,
        "no_trade_day": False,
        "no_trade_reasons": [],
        "adaptive_notes": [],
        "adaptive_threshold": ALERT_THRESHOLD,
    }

    no_trade, no_trade_reasons = detect_no_trade_conditions(result)
    if no_trade:
        result["signal"] = "NONE"
        result["strength"] = "❌ WEAK"
        result["entry"] = "No Trade"
        result["expected_move"] = "Avoid / No Trade Day"
        result["stop_loss"] = 0
        result["target_price"] = 0
        result["trailing_sl"] = 0
        result["entry_trigger_price"] = 0
        result["strikes"] = {}
        result["no_trade_day"] = True
        result["no_trade_reasons"] = no_trade_reasons

    adaptive = get_adaptive_settings()
    result["adaptive_notes"] = adaptive["notes"]
    result["adaptive_threshold"] = ALERT_THRESHOLD + adaptive["threshold_boost"]

    if adaptive["hard_block"]:
        result["signal"] = "NONE"
        result["strength"] = "❌ WEAK"
        result["entry"] = "No Trade"
        result["expected_move"] = "Adaptive Risk Lock"
        result["stop_loss"] = 0
        result["target_price"] = 0
        result["trailing_sl"] = 0
        result["entry_trigger_price"] = 0
        result["strikes"] = {}
    elif adaptive["confidence_penalty"] > 0:
        result["confidence"] = max(0, int(result["confidence"] - adaptive["confidence_penalty"]))
        if result["confidence"] < result["adaptive_threshold"]:
            result["signal"] = "NONE"
            result["strength"] = "❌ WEAK"
            result["entry"] = "No Trade"
            result["expected_move"] = "Adaptive Filtered"
            result["stop_loss"] = 0
            result["target_price"] = 0
            result["trailing_sl"] = 0
            result["entry_trigger_price"] = 0
            result["strikes"] = {}

    msg = (
        f"{name}\n\n"
        f"Price: {result['price']}\n"
        f"Market Regime: {result['regime']}\n"
        f"Session: {result['session']}\n\n"
        f"Trend: {result['trend']}\n"
        f"Higher TF Trend: {result['htf_trend']}\n"
        f"RSI: {result['rsi']}\n"
        f"EMA20: {result['ema20']}\n"
        f"EMA50: {result['ema50']}\n"
        f"ATR: {result['atr']}\n"
        f"ADX: {result['adx']}\n"
        f"VWAP: {result['vwap']}\n\n"
        f"Compression: {result['compression']}\n"
        f"Vol Expansion: {result['vol_expansion']}\n"
        f"Volume Breakout: {result['volume_breakout']}\n"
        f"Liquidity Trap: {result['liquidity_trap']}\n"
        f"Breakout Confirmed: {result['breakout_confirmed']}\n"
        f"Breakout Direction: {result['breakout_direction']}\n"
        f"ORB: {result['orb_break']} ({result['orb_direction']})\n"
        f"VWAP Behavior: {result['vwap_behavior']}\n"
        f"Strong Candle: {result['strong_candle']}\n\n"
        f"Market Bias Proxy: {result['options_bias']}\n"
        f"PCR Proxy: {result['pcr']}\n\n"
        f"Liquidity Target: {result['liquidity_target']}\n"
        f"Distance: {result['distance']} pts\n\n"
        f"Confidence: {result['confidence']}%\n"
        f"Signal: {result['signal']}\n"
        f"Expected Move: {result['expected_move']}\n"
        f"Stop Loss: {result['stop_loss']}\n"
        f"Target: {result['target_price']}\n"
        f"Trailing SL: {result['trailing_sl']}\n"
        f"Strength: {result['strength']}\n"
        f"Entry: {result['entry']}\n"
        f"Trigger Price: {result['entry_trigger_price']}\n"
        f"Ultra Confirmation: {ultra_confirmation}"
    )

    if filter_notes:
        msg += f"\n\n🧠 Logic: {', '.join(filter_notes)}"

    if result["no_trade_day"]:
        msg += f"\n\n🚫 NO TRADE DAY\nReason: {', '.join(result['no_trade_reasons'])}"

    if adaptive["hard_block"]:
        msg += f"\n\n🛑 ADAPTIVE RISK LOCK ACTIVE\nReason: {', '.join(adaptive['notes']) if adaptive['notes'] else 'Risk Protection'}"
    elif adaptive["confidence_penalty"] > 0:
        msg += f"\n\n⚙ Adaptive Adjustment: -{adaptive['confidence_penalty']} confidence"
        if adaptive["notes"]:
            msg += f"\nAdaptive Reason: {', '.join(adaptive['notes'])}"
    elif adaptive["threshold_boost"] < 0 and adaptive["notes"]:
        msg += f"\n\n⚙ Adaptive Adjustment: Aggressive Mode ({', '.join(adaptive['notes'])})"

    if result["signal"] != "NONE" and result["strikes"]:
        msg += (
            f"\n\n🎯 Option Strikes:\n"
            f"ATM: {result['strikes']['ATM']}\n"
            f"ITM: {result['strikes']['ITM']}\n"
            f"OTM: {result['strikes']['OTM']}"
        )

    result["message"] = msg
    return result


# =========================================================
# OUTCOME TRACKER
# =========================================================
def fetch_outcome_data(symbol: str) -> pd.DataFrame:
    try:
        data = yf.download(
            symbol,
            interval=DEFAULT_INTERVAL,
            period=DEFAULT_PERIOD,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return normalize_ohlc_dataframe(data)
    except Exception as e:
        logger.warning("Failed outcome fetch for %s: %s", symbol, e)
        return pd.DataFrame()


def compute_rr(signal: str, entry_price: float, stop_loss: float, exit_price: float) -> float:
    try:
        if signal == "CALL":
            risk = entry_price - stop_loss
            reward = exit_price - entry_price
        else:
            risk = stop_loss - entry_price
            reward = entry_price - exit_price

        if risk <= 0:
            return 0.0
        return round(reward / risk, 2)
    except Exception:
        return 0.0


def update_trade_outcomes():
    try:
        df = load_trade_log_df()
        if df.empty:
            return

        changed = False
        now = datetime.now(IST)

        for idx, row in df.iterrows():
            status = str(row.get("status", ""))
            if status not in ["PENDING", "ENTERED"]:
                continue

            signal = str(row.get("signal", "NONE"))
            yahoo_symbol = str(row.get("yahoo_symbol", ""))
            if signal not in ["CALL", "PUT"] or not yahoo_symbol:
                continue

            signal_time_str = str(row.get("signal_time", ""))
            try:
                signal_time = datetime.fromisoformat(signal_time_str)
            except Exception:
                continue

            if signal_time.tzinfo is None:
                signal_time = signal_time.replace(tzinfo=IST)
            else:
                signal_time = signal_time.astimezone(IST)

            if now - signal_time > timedelta(days=TRADE_MAX_AGE_DAYS):
                if status == "PENDING":
                    df.at[idx, "status"] = "EXPIRED"
                    df.at[idx, "outcome"] = "NOT_TRIGGERED"
                    df.at[idx, "notes"] = "Trade expired without trigger"
                    changed = True
                elif status == "ENTERED" and pd.isna(row.get("exit_time")):
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "TIME_EXIT"
                    changed = True
                continue

            market_data = fetch_outcome_data(yahoo_symbol)
            if market_data.empty:
                continue

            if market_data.index.tz is None:
                market_data.index = market_data.index.tz_localize(IST)
            else:
                market_data.index = market_data.index.tz_convert(IST)

            future = market_data[market_data.index > signal_time]
            if future.empty:
                continue

            trigger_price = safe_float(row.get("trigger_price", row.get("price_at_signal", 0)))
            stop_loss = safe_float(row.get("stop_loss", 0))
            target_price = safe_float(row.get("target_price", 0))
            entry_price_row = row.get("entry_price", "")
            entry_time_row = row.get("entry_time", "")

            entered = status == "ENTERED" and str(entry_time_row).strip() != ""

            entry_price = safe_float(entry_price_row, 0) if entered else 0.0
            entry_time = None
            if entered:
                try:
                    entry_time = datetime.fromisoformat(str(entry_time_row))
                    if entry_time.tzinfo is None:
                        entry_time = entry_time.replace(tzinfo=IST)
                    else:
                        entry_time = entry_time.astimezone(IST)
                except Exception:
                    entered = False

            if not entered:
                for ts, candle in future.iterrows():
                    high_val = safe_float(candle["High"])
                    low_val = safe_float(candle["Low"])

                    if signal == "CALL" and high_val >= trigger_price:
                        entry_price = trigger_price
                        entry_time = ts.to_pydatetime()
                        df.at[idx, "status"] = "ENTERED"
                        df.at[idx, "entry_price"] = entry_price
                        df.at[idx, "entry_time"] = entry_time.isoformat()
                        changed = True
                        entered = True
                        break

                    if signal == "PUT" and low_val <= trigger_price:
                        entry_price = trigger_price
                        entry_time = ts.to_pydatetime()
                        df.at[idx, "status"] = "ENTERED"
                        df.at[idx, "entry_price"] = entry_price
                        df.at[idx, "entry_time"] = entry_time.isoformat()
                        changed = True
                        entered = True
                        break

            if not entered or entry_time is None:
                continue

            post_entry = market_data[market_data.index >= entry_time]
            if post_entry.empty:
                continue

            for ts, candle in post_entry.iterrows():
                high_val = safe_float(candle["High"])
                low_val = safe_float(candle["Low"])
                close_val = safe_float(candle["Close"])

                if signal == "CALL":
                    sl_hit = low_val <= stop_loss
                    tp_hit = high_val >= target_price
                else:
                    sl_hit = high_val >= stop_loss
                    tp_hit = low_val <= target_price

                if sl_hit and tp_hit:
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "SL_FIRST_SAME_CANDLE"
                    df.at[idx, "exit_price"] = stop_loss
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, stop_loss)
                    df.at[idx, "notes"] = "Both SL and TP touched same candle; conservative SL assumed"
                    changed = True
                    break

                if sl_hit:
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "STOP_LOSS_HIT"
                    df.at[idx, "exit_price"] = stop_loss
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, stop_loss)
                    changed = True
                    break

                if tp_hit:
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "TARGET_HIT"
                    df.at[idx, "exit_price"] = target_price
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, target_price)
                    changed = True
                    break

                if now - entry_time > timedelta(days=1):
                    df.at[idx, "status"] = "CLOSED"
                    df.at[idx, "outcome"] = "TIME_EXIT"
                    df.at[idx, "exit_price"] = close_val
                    df.at[idx, "exit_time"] = ts.to_pydatetime().isoformat()
                    df.at[idx, "rr"] = compute_rr(signal, entry_price, stop_loss, close_val)
                    df.at[idx, "notes"] = "Closed by time-based exit"
                    changed = True
                    break

        if changed:
            save_trade_log_df(df)

    except Exception as e:
        logger.warning("Outcome tracker failed: %s", e)


def get_daily_stats_message() -> str:
    df = load_trade_log_df()
    if df.empty:
        return "📊 DAILY STATS\n\nNo trade data yet"

    df["signal_time"] = pd.to_datetime(df["signal_time"], errors="coerce")
    if str(df["signal_time"].dt.tz) == "None":
        df["signal_time"] = df["signal_time"].dt.tz_localize(IST)
    else:
        df["signal_time"] = df["signal_time"].dt.tz_convert(IST)

    today = datetime.now(IST).date()
    today_df = df[df["signal_time"].dt.date == today].copy()

    if today_df.empty:
        return "📊 DAILY STATS\n\nNo signals today"

    total = len(today_df)
    entered = len(today_df[today_df["status"] == "ENTERED"])
    closed = len(today_df[today_df["status"] == "CLOSED"])
    expired = len(today_df[today_df["status"] == "EXPIRED"])
    targets = len(today_df[today_df["outcome"] == "TARGET_HIT"])
    stops = len(today_df[today_df["outcome"].isin(["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"])])
    time_exits = len(today_df[today_df["outcome"] == "TIME_EXIT"])

    rr_series = pd.to_numeric(today_df["rr"], errors="coerce")
    avg_rr = round(rr_series.dropna().mean(), 2) if rr_series.notna().any() else 0.0

    win_rate = round((targets / closed) * 100, 2) if closed > 0 else 0.0

    counts_by_symbol = today_df.groupby("symbol").size().sort_values(ascending=False)
    best_symbol = counts_by_symbol.index[0] if not counts_by_symbol.empty else "N/A"

    return (
        "📊 DAILY STATS\n\n"
        f"Signals Today: {total}\n"
        f"Entered: {entered}\n"
        f"Closed: {closed}\n"
        f"Expired: {expired}\n"
        f"Target Hits: {targets}\n"
        f"Stop Loss Hits: {stops}\n"
        f"Time Exits: {time_exits}\n"
        f"Win Rate: {win_rate}%\n"
        f"Average RR: {avg_rr}\n"
        f"Most Active Index: {best_symbol}"
    )


# =========================================================
# ALERTS
# =========================================================
def build_signal_summary(result: dict) -> str:
    return (
        f"{result['name']}\n"
        f"Price: {result['price']}\n"
        f"Trend: {result['trend']}\n"
        f"Higher TF Trend: {result['htf_trend']}\n"
        f"Regime: {result['regime']}\n"
        f"Session: {result['session']}\n"
        f"Signal: {result['signal']}\n"
        f"Confidence: {result['confidence']}%\n"
        f"Expected Move: {result['expected_move']}\n"
        f"Strength: {result['strength']}\n"
        f"Entry: {result['entry']}\n"
        f"Trigger: {result['entry_trigger_price']}\n"
        f"ADX: {result['adx']}\n"
        f"VWAP: {result['vwap']}\n"
        f"Volume Breakout: {result['volume_breakout']}\n"
        f"SL: {result['stop_loss']}\n"
        f"Target: {result['target_price']}\n"
        f"Trailing SL: {result['trailing_sl']}"
    )


def should_send_alert(result: dict) -> bool:
    if result.get("session") == "STALE DATA":
        return False
    if result.get("no_trade_day", False):
        return False
    if result.get("signal") == "NONE":
        return False
    if result.get("strength") == "❌ WEAK":
        return False
    if safe_float(result.get("confidence", 0), 0) < safe_float(result.get("adaptive_threshold", ALERT_THRESHOLD), ALERT_THRESHOLD):
        return False
    if result.get("expected_move") in [
        "Market Closed",
        "Avoid / Low Probability",
        "Avoid This Session",
        "Unknown",
        "Blocked - Stale Data",
        "Avoid / No Trade Day",
        "Adaptive Risk Lock",
        "Adaptive Filtered",
    ]:
        return False
    if result.get("entry") not in ["Enter Now", "Wait Breakout", "Wait Breakdown"]:
        return False
    return True


def process_alert_logging(result: dict):
    try:
        log_trade(result)
    except Exception as e:
        logger.warning("Alert logging failed: %s", e)


def is_duplicate_alert(result: dict) -> bool:
    key = f"{result['name']}|{result['signal']}|{result['regime']}"
    now = datetime.now(IST)

    if key not in LAST_ALERTS:
        LAST_ALERTS[key] = now
        save_alert_state()
        return False

    last_time = LAST_ALERTS[key]
    if now - last_time < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
        return True

    LAST_ALERTS[key] = now
    save_alert_state()
    return False


# =========================================================
# SCAN RUNNER
# =========================================================
async def run_full_scan_once() -> list:
    results = [
        analyze_market(SYMBOLS["NIFTY 50"], "NIFTY 50"),
        analyze_market(SYMBOLS["BANK NIFTY"], "BANK NIFTY"),
        analyze_market(SYMBOLS["SENSEX"], "SENSEX"),
    ]
    cache_scan_results(results)
    return results


# =========================================================
# COMMANDS
# =========================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    await update.message.reply_text(
        "🚀 PRO TRADING BOT ACTIVE\n"
        "Commands:\n"
        "/scan\n"
        "/forcescan\n"
        "/summary\n"
        "/besttrade\n"
        "/why\n"
        "/performance\n"
        "/dailystats\n"
        "/notrade\n"
        "/adaptive\n"
        "/nifty\n"
        "/banknifty\n"
        "/sensex\n"
        "/signal\n"
        "/mode\n"
        "/settings\n"
        "/status\n"
        "/health\n"
        "/resetalerts\n"
        "/setthreshold 75\n"
        "/setcooldown 60\n"
        "/setalertmode on\n"
        "/setalertmode off"
    )


async def mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    await update.message.reply_text(
        f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
        f"Alert Threshold: {ALERT_THRESHOLD}%\n"
        f"Alert Cooldown: {ALERT_COOLDOWN_MINUTES} minutes"
    )


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    await update.message.reply_text(
        "⚙️ CURRENT SETTINGS\n\n"
        f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
        f"Alert Threshold: {ALERT_THRESHOLD}%\n"
        f"Alert Cooldown: {ALERT_COOLDOWN_MINUTES} minutes\n"
        f"State File: {ALERT_STATE_FILE}\n"
        f"Config File: {CONFIG_FILE}\n"
        f"Status File: {STATUS_FILE}\n"
        f"Trade Log File: {TRADE_LOG_FILE}"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %I:%M:%S %p IST")

    await update.message.reply_text(
        "🟢 BOT STATUS\n\n"
        f"Alive: Yes\n"
        f"Current Time: {now_ist}\n"
        f"Last Scan: {fmt_dt(LAST_SCAN_TIME)}\n"
        f"Last Alert: {fmt_dt(LAST_ALERT_TIME)}\n"
        f"Alerts Sent Today: {ALERTS_SENT_TODAY}\n"
        f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
        f"Alert Threshold: {ALERT_THRESHOLD}%\n"
        f"Alert Cooldown: {ALERT_COOLDOWN_MINUTES} minutes"
    )


async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    try:
        nifty_data = fetch_market_data(SYMBOLS["NIFTY 50"])
        bank_data = fetch_market_data(SYMBOLS["BANK NIFTY"])
        sensex_data = fetch_market_data(SYMBOLS["SENSEX"])

        nifty_age = round(get_data_age_minutes(nifty_data), 2) if not nifty_data.empty else -1
        bank_age = round(get_data_age_minutes(bank_data), 2) if not bank_data.empty else -1
        sensex_age = round(get_data_age_minutes(sensex_data), 2) if not sensex_data.empty else -1

        message = (
            "🩺 BOT HEALTH CHECK\n\n"
            f"Market Weekday: {is_market_weekday()}\n"
            f"Market Hours Now: {is_market_hours_now()}\n"
            f"Last Scan: {fmt_dt(LAST_SCAN_TIME)}\n"
            f"Last Alert: {fmt_dt(LAST_ALERT_TIME)}\n"
            f"Alerts Sent Today: {ALERTS_SENT_TODAY}\n\n"
            f"NIFTY Data Age: {nifty_age} min\n"
            f"BANKNIFTY Data Age: {bank_age} min\n"
            f"SENSEX Data Age: {sensex_age} min\n\n"
            f"Alert Only Mode: {ALERT_ONLY_MODE}\n"
            f"Alert Threshold: {ALERT_THRESHOLD}%\n"
            f"Cooldown: {ALERT_COOLDOWN_MINUTES} min"
        )

        await update.message.reply_text(message)
    except Exception as e:
        await update.message.reply_text(f"❌ Health check failed: {e}")


async def resetalerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_ALERTS, ALERTS_SENT_TODAY, LAST_ALERT_TIME

    if not await require_authorized(update):
        return

    LAST_ALERTS = {}
    ALERTS_SENT_TODAY = 0
    LAST_ALERT_TIME = None

    save_alert_state()
    save_status()

    await update.message.reply_text("✅ Alert memory reset successfully")


async def setthreshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ALERT_THRESHOLD

    if not await require_authorized(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setthreshold 75")
        return

    try:
        value = int(context.args[0])

        if value < 0 or value > 100:
            await update.message.reply_text("Threshold must be between 0 and 100.")
            return

        ALERT_THRESHOLD = value
        save_config()
        await update.message.reply_text(f"✅ Alert threshold updated to {ALERT_THRESHOLD}%")
    except ValueError:
        await update.message.reply_text("Please enter a valid integer. Example: /setthreshold 75")


async def setcooldown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ALERT_COOLDOWN_MINUTES

    if not await require_authorized(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setcooldown 60")
        return

    try:
        value = int(context.args[0])

        if value < 1 or value > 1440:
            await update.message.reply_text("Cooldown must be between 1 and 1440 minutes.")
            return

        ALERT_COOLDOWN_MINUTES = value
        save_config()
        await update.message.reply_text(f"✅ Alert cooldown updated to {ALERT_COOLDOWN_MINUTES} minutes")
    except ValueError:
        await update.message.reply_text("Please enter a valid integer. Example: /setcooldown 60")


async def setalertmode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ALERT_ONLY_MODE

    if not await require_authorized(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setalertmode on  or  /setalertmode off")
        return

    value = context.args[0].strip().lower()

    if value == "on":
        ALERT_ONLY_MODE = True
        save_config()
        await update.message.reply_text("✅ Alert-only mode is now ON")
        return

    if value == "off":
        ALERT_ONLY_MODE = False
        save_config()
        await update.message.reply_text("✅ Alert-only mode is now OFF")
        return

    await update.message.reply_text("Usage: /setalertmode on  or  /setalertmode off")


async def signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    if LAST_SCAN_RESULTS:
        nifty_result = LAST_SCAN_RESULTS.get("NIFTY 50", analyze_market(SYMBOLS["NIFTY 50"], "NIFTY 50"))
        bank_result = LAST_SCAN_RESULTS.get("BANK NIFTY", analyze_market(SYMBOLS["BANK NIFTY"], "BANK NIFTY"))
        sensex_result = LAST_SCAN_RESULTS.get("SENSEX", analyze_market(SYMBOLS["SENSEX"], "SENSEX"))
    else:
        results = await run_full_scan_once()
        nifty_result, bank_result, sensex_result = results

    message = (
        "📌 QUICK SIGNAL SUMMARY\n\n"
        f"{build_signal_summary(nifty_result)}\n\n"
        f"{build_signal_summary(bank_result)}\n\n"
        f"{build_signal_summary(sensex_result)}"
    )
    await update.message.reply_text(message)


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    results = await run_full_scan_once()

    message = (
        "📊 MANUAL MARKET SCAN\n\n"
        f"{results[0]['message']}\n\n"
        f"{results[1]['message']}\n\n"
        f"{results[2]['message']}"
    )
    await update.message.reply_text(message)


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    await update.message.reply_text(LAST_SCAN_SUMMARY_TEXT)


async def besttrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    results = list(LAST_SCAN_RESULTS.values())
    if not results:
        results = await run_full_scan_once()

    best = get_best_signal_result(results)
    await update.message.reply_text(build_best_trade_message(best))


async def forcescan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_SCAN_TIME

    if not await require_authorized(update):
        return

    if SCAN_LOCK.locked():
        await update.message.reply_text("⏳ Scan already running")
        return

    async with SCAN_LOCK:
        try:
            LAST_SCAN_TIME = datetime.now(IST)
            save_status()

            results = await run_full_scan_once()

            message = (
                "⚡ FORCE SCAN COMPLETE\n\n"
                f"{results[0]['message']}\n\n"
                f"{results[1]['message']}\n\n"
                f"{results[2]['message']}"
            )
            await update.message.reply_text(message)

        except Exception as e:
            logger.exception("Force scan failed: %s", e)
            await update.message.reply_text(f"❌ Force scan failed: {e}")


async def why(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    if not LAST_SCAN_RESULTS:
        await update.message.reply_text("Run /scan first")
        return

    msg = ["🧠 WHY SIGNAL ANALYSIS\n"]

    for name, r in LAST_SCAN_RESULTS.items():
        explanation = []

        if r.get("signal") == "NONE":
            explanation.append("❌ No Trade")

        if r.get("trend") == r.get("htf_trend") and r.get("trend") != "Neutral":
            explanation.append("✅ Trend aligned with HTF")

        if safe_float(r.get("adx", 0), 0) >= 18:
            explanation.append("✅ Strong Trend (ADX)")
        else:
            explanation.append("⚠ ADX Weak")

        if r.get("signal") == "CALL" and safe_float(r.get("price", 0), 0) > safe_float(r.get("vwap", 0), 0):
            explanation.append("✅ Above VWAP")

        if r.get("signal") == "PUT" and safe_float(r.get("price", 0), 0) < safe_float(r.get("vwap", 0), 0):
            explanation.append("✅ Below VWAP")

        if r.get("volume_breakout"):
            explanation.append("✅ Volume Breakout")
        else:
            explanation.append("⚠ No Volume Breakout")

        if r.get("orb_break"):
            explanation.append(f"✅ ORB {r.get('orb_direction','NONE')}")

        if r.get("vwap_behavior") == "RECLAIM":
            explanation.append("✅ VWAP Reclaim")

        if r.get("vwap_behavior") == "REJECTION":
            explanation.append("✅ VWAP Rejection")

        if r.get("strong_candle"):
            explanation.append("✅ Strong Candle")
        else:
            explanation.append("⚠ Weak Candle")

        if r.get("regime") == "TREND DAY":
            explanation.append("🔥 Trend Day")

        if r.get("regime") == "BREAKOUT DAY":
            explanation.append("🔥 Breakout Day")

        if r.get("no_trade_day", False):
            explanation.append("🚫 No Trade Day")

        adaptive_notes = r.get("adaptive_notes", [])
        if adaptive_notes:
            explanation.append(f"⚙ Adaptive: {', '.join(adaptive_notes)}")

        msg.append(f"{name}")
        msg.append(" | ".join(explanation))
        msg.append("")

    await update.message.reply_text("\n".join(msg))


async def performance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    df = load_trade_log_df()
    if df.empty:
        await update.message.reply_text("No trades logged")
        return

    total = len(df)
    calls = len(df[df["signal"] == "CALL"])
    puts = len(df[df["signal"] == "PUT"])
    closed = len(df[df["status"] == "CLOSED"])
    targets = len(df[df["outcome"] == "TARGET_HIT"])
    stops = len(df[df["outcome"].isin(["STOP_LOSS_HIT", "SL_FIRST_SAME_CANDLE"])])
    expired = len(df[df["status"] == "EXPIRED"])

    rr_series = pd.to_numeric(df["rr"], errors="coerce")
    avg_conf = round(pd.to_numeric(df["confidence"], errors="coerce").mean(), 2) if total > 0 else 0.0
    avg_rr = round(rr_series.dropna().mean(), 2) if rr_series.notna().any() else 0.0
    win_rate = round((targets / closed) * 100, 2) if closed > 0 else 0.0

    msg = (
        "📊 PERFORMANCE STATS\n\n"
        f"Total Trades Logged: {total}\n"
        f"CALL Signals: {calls}\n"
        f"PUT Signals: {puts}\n"
        f"Closed Trades: {closed}\n"
        f"Target Hits: {targets}\n"
        f"Stop Loss Hits: {stops}\n"
        f"Expired Trades: {expired}\n"
        f"Win Rate: {win_rate}%\n"
        f"Avg Confidence: {avg_conf}%\n"
        f"Avg RR: {avg_rr}\n"
    )

    await update.message.reply_text(msg)


async def dailystats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    await update.message.reply_text(get_daily_stats_message())


async def notrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    results = list(LAST_SCAN_RESULTS.values())
    if not results:
        results = await run_full_scan_once()

    lines = ["🚫 NO TRADE ENGINE REPORT", ""]
    for r in results:
        reasons = r.get("no_trade_reasons", [])
        status = "YES" if r.get("no_trade_day", False) else "NO"
        lines.append(f"{r.get('name', 'UNKNOWN')}")
        lines.append(f"No Trade Day: {status}")
        lines.append(f"Reasons: {', '.join(reasons) if reasons else 'None'}")
        lines.append("")

    await update.message.reply_text("\n".join(lines))


async def adaptive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return

    adaptive_info = get_adaptive_settings()
    metrics = adaptive_info["metrics"]

    message = (
        "⚙ ADAPTIVE BEHAVIOR REPORT\n\n"
        f"Recent Trades Checked: {metrics['total']}\n"
        f"Closed Trades: {metrics['closed']}\n"
        f"Target Hits: {metrics['targets']}\n"
        f"Stop Losses: {metrics['stops']}\n"
        f"Win Rate: {metrics['win_rate']}%\n"
        f"Avg RR: {metrics['avg_rr']}\n"
        f"Consecutive Losses: {metrics['consecutive_losses']}\n\n"
        f"Threshold Boost: {adaptive_info['threshold_boost']}\n"
        f"Confidence Penalty: {adaptive_info['confidence_penalty']}\n"
        f"Hard Block: {adaptive_info['hard_block']}\n"
        f"Notes: {', '.join(adaptive_info['notes']) if adaptive_info['notes'] else 'None'}"
    )
    await update.message.reply_text(message)


async def nifty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    result = analyze_market(SYMBOLS["NIFTY 50"], "NIFTY 50")
    await update.message.reply_text(result["message"])


async def banknifty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    result = analyze_market(SYMBOLS["BANK NIFTY"], "BANK NIFTY")
    await update.message.reply_text(result["message"])


async def sensex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_authorized(update):
        return
    result = analyze_market(SYMBOLS["SENSEX"], "SENSEX")
    await update.message.reply_text(result["message"])


# =========================================================
# AUTO SCAN
# =========================================================
async def auto_scan(context: ContextTypes.DEFAULT_TYPE):
    global LAST_SCAN_TIME, LAST_ALERT_TIME, ALERTS_SENT_TODAY

    if SCAN_LOCK.locked():
        logger.info("Skipping auto scan because previous scan is still running")
        return

    async with SCAN_LOCK:
        try:
            reset_daily_alert_counter_if_needed()
            cleanup_old_alerts()
            update_trade_outcomes()

            LAST_SCAN_TIME = datetime.now(IST)
            save_status()

            if ALERT_ONLY_MODE and (not is_market_open_day() or not is_active_market_session()):
                logger.info("Skipping alert-only auto scan outside active market session")
                return

            results = await run_full_scan_once()

            if ALERT_ONLY_MODE:
                for result in results:
                    if should_send_alert(result) and not is_duplicate_alert(result):
                        alert_message = (
                            "🚨 HIGH CONFIDENCE ALERT\n\n"
                            f"{result['message']}"
                        )
                        await context.bot.send_message(chat_id=CHAT_ID, text=alert_message)
                        LAST_ALERT_TIME = datetime.now(IST)
                        ALERTS_SENT_TODAY += 1
                        process_alert_logging(result)
                        save_status()
            else:
                message = (
                    "📊 MARKET SCAN\n\n"
                    f"{results[0]['message']}\n\n"
                    f"{results[1]['message']}\n\n"
                    f"{results[2]['message']}"
                )
                await context.bot.send_message(chat_id=CHAT_ID, text=message)

        except Exception as e:
            logger.exception("Auto scan failed: %s", e)
            try:
                await context.bot.send_message(chat_id=CHAT_ID, text=f"Error: {e}")
            except Exception:
                logger.exception("Failed to send error message to Telegram")


# =========================================================
# MAIN
# =========================================================
def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing. Set it in your environment variables.")

    if not CHAT_ID:
        raise ValueError("CHAT_ID is missing. Set it in your environment variables.")

    if AUTHORIZED_CHAT_ID == 0:
        raise ValueError("AUTHORIZED_CHAT_ID is missing or invalid. Set it as an integer environment variable.")

    load_config()
    load_alert_state()
    load_status()
    reset_daily_alert_counter_if_needed()
    init_trade_log()
    update_trade_outcomes()

    logger.info("Starting bot with DATA_DIR=%s", DATA_DIR)
    logger.info("Alert only mode=%s | threshold=%s | cooldown=%s", ALERT_ONLY_MODE, ALERT_THRESHOLD, ALERT_COOLDOWN_MINUTES)

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mode", mode))
    app.add_handler(CommandHandler("settings", settings))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("resetalerts", resetalerts))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("besttrade", besttrade))
    app.add_handler(CommandHandler("forcescan", forcescan))
    app.add_handler(CommandHandler("why", why))
    app.add_handler(CommandHandler("performance", performance))
    app.add_handler(CommandHandler("dailystats", dailystats))
    app.add_handler(CommandHandler("notrade", notrade))
    app.add_handler(CommandHandler("adaptive", adaptive))
    app.add_handler(CommandHandler("setthreshold", setthreshold))
    app.add_handler(CommandHandler("setcooldown", setcooldown))
    app.add_handler(CommandHandler("setalertmode", setalertmode))
    app.add_handler(CommandHandler("signal", signal))
    app.add_handler(CommandHandler("scan", scan))
    app.add_handler(CommandHandler("nifty", nifty))
    app.add_handler(CommandHandler("banknifty", banknifty))
    app.add_handler(CommandHandler("sensex", sensex))

    
    if app.job_queue is None:
        raise RuntimeError("JobQueue is not available. Install python-telegram-bot with job-queue support.")

    app.job_queue.run_repeating(auto_scan, interval=900, first=10)

    print("Bot running...")
    app.run_polling()


# =========================================================
# 🚀 AGGRESSIVE SMART MODE (SAFE INJECTION)
# =========================================================

def aggressive_signal_boost(result: dict) -> dict:
    try:
        signal = result.get("signal", "NONE")
        trend = result.get("trend", "Neutral")
        htf_trend = result.get("htf_trend", "Neutral")
        adx = result.get("adx", 0)
        price = result.get("price", 0)
        vwap = result.get("vwap", 0)
        confidence = result.get("confidence", 0)
        regime = result.get("regime", "UNKNOWN")

        # Only activate on strong structure
        if regime not in ["TREND DAY", "BREAKOUT DAY"]:
            return result

        # =================================================
        # 🔥 TREND CONTINUATION ENTRY (MAIN FIX)
        # =================================================
        if signal == "NONE":

            if trend == "Bullish" and adx >= 25 and price > vwap:
                result["signal"] = "CALL"
                result["entry"] = "Trend Continuation"
                confidence += 15
                result["notes"] = "Aggressive BUY"

            elif trend == "Bearish" and adx >= 25 and price < vwap:
                result["signal"] = "PUT"
                result["entry"] = "Trend Continuation"
                confidence += 15
                result["notes"] = "Aggressive SELL"

        # =================================================
        # ⚡ RELAX HTF FILTER
        # =================================================
        if result.get("signal") != "NONE" and trend != htf_trend:
            confidence -= 5

        # =================================================
        # ⚡ RELAX STRICT FILTERS
        # =================================================
        if result.get("signal") != "NONE":

            if result.get("volume_breakout") is False:
                confidence += 5

            if result.get("strength") == "❌ WEAK":
                confidence += 10

        # =================================================
        # 🎯 FINAL CONFIDENCE
        # =================================================
        confidence = max(min(confidence, 95), 50)
        result["confidence"] = confidence

        if confidence >= 85:
            result["strength"] = "🔥 EXTREME"
        elif confidence >= 75:
            result["strength"] = "💪 STRONG"
        elif confidence >= 60:
            result["strength"] = "⚠ MODERATE"
        else:
            result["strength"] = "❌ WEAK"

        return result

    except Exception as e:
        print("Aggressive mode error:", e)
        return result
# =========================================================
# 🚀 TREND CONTINUATION ENTRY LOGIC (APPEND MODE)
# Paste this ABOVE: if __name__ == "__main__": main()
# =========================================================

def detect_trend_continuation_entry(
    trend: str,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    open_: pd.Series,
    vwap: pd.Series,
    adx_value: float,
    session_name: str,
) -> str:
    try:
        if len(close) < 4 or len(high) < 4 or len(low) < 4 or len(open_) < 4 or len(vwap) < 4:
            return "NONE"

        price = safe_float(close.iloc[-1])
        prev_close = safe_float(close.iloc[-2])
        prev_high = safe_float(high.iloc[-2])
        prev_low = safe_float(low.iloc[-2])

        last_open = safe_float(open_.iloc[-1])
        last_high = safe_float(high.iloc[-1])
        last_low = safe_float(low.iloc[-1])

        vwap_now = safe_float(vwap.iloc[-1])

        last_range = max(last_high - last_low, 0.01)
        last_body = abs(price - last_open)
        body_ratio = last_body / last_range

        strong_session = session_name in [
            "PRIME TREND WINDOW",
            "AFTERNOON BUILDUP",
            "CLOSING MOVE WINDOW",
        ]

        if adx_value < 22:
            return "NONE"

        # Bullish continuation
        if trend == "Bullish" and price > vwap_now and strong_session:
            # breakout continuation
            if price > prev_high:
                return "CALL"
            # strong bullish hold candle above previous close
            if price > last_open and price >= prev_close and body_ratio >= 0.45:
                return "CALL"

        # Bearish continuation
        if trend == "Bearish" and price < vwap_now and strong_session:
            # breakdown continuation
            if price < prev_low:
                return "PUT"
            # strong bearish hold candle below previous close
            if price < last_open and price <= prev_close and body_ratio >= 0.45:
                return "PUT"

        return "NONE"
    except Exception as e:
        logger.warning("Trend continuation entry detection failed: %s", e)
        return "NONE"


_original_generate_signal = generate_signal

def generate_signal(
    symbol: str,
    trend: str,
    momentum: float,
    regime: str,
    trading_allowed: bool,
    liquidity_trap: bool,
    trap_direction: str,
    session_name: str,
    breakout_confirmed: bool,
    breakout_direction: str,
    close: pd.Series,
    open_: pd.Series,
    high: pd.Series,
    low: pd.Series,
) -> str:
    signal = _original_generate_signal(
        symbol=symbol,
        trend=trend,
        momentum=momentum,
        regime=regime,
        trading_allowed=trading_allowed,
        liquidity_trap=liquidity_trap,
        trap_direction=trap_direction,
        session_name=session_name,
        breakout_confirmed=breakout_confirmed,
        breakout_direction=breakout_direction,
        close=close,
        open_=open_,
        high=high,
        low=low,
    )

    # Keep original decision if already valid
    if signal != "NONE":
        return signal

    # Continuation logic only in valid market structure
    if session_name == "MARKET CLOSED":
        return "NONE"

    if regime not in ["TREND DAY", "BREAKOUT DAY"]:
        return "NONE"

    if not trading_allowed:
        return "NONE"

    if liquidity_trap and trap_direction != "NONE":
        return "NONE"

    if not detect_momentum_alignment(trend, momentum):
        return "NONE"

    # Relax HTF for strong intraday continuation
    htf_trend = get_higher_timeframe_trend(symbol)
    htf_conflict = htf_trend != "Neutral" and trend != htf_trend

    # We need VWAP + ADX here, so compute lightweight versions
    try:
        volume_proxy = pd.Series([1.0] * len(close), index=close.index)
        vwap_series = calculate_vwap(high, low, close, volume_proxy)
        adx_series = calculate_adx(high, low, close)
        adx_value = safe_float(adx_series.iloc[-1], 0.0)
    except Exception:
        return "NONE"

    continuation_signal = detect_trend_continuation_entry(
        trend=trend,
        close=close,
        high=high,
        low=low,
        open_=open_,
        vwap=vwap_series,
        adx_value=adx_value,
        session_name=session_name,
    )

    if continuation_signal == "NONE":
        return "NONE"

    # If HTF conflicts, still allow continuation on strong ADX/session
    if htf_conflict and adx_value < 28 and session_name != "CLOSING MOVE WINDOW":
        return "NONE"

    logger.info(
        "TREND CONTINUATION ENTRY ACTIVE | %s | trend=%s | htf=%s | adx=%.2f | session=%s | signal=%s",
        symbol,
        trend,
        htf_trend,
        adx_value,
        session_name,
        continuation_signal,
    )
    return continuation_signal


_original_aggressive_signal_boost = aggressive_signal_boost

def aggressive_signal_boost(result: dict) -> dict:
    result = _original_aggressive_signal_boost(result)

    try:
        signal = result.get("signal", "NONE")
        trend = result.get("trend", "Neutral")
        htf_trend = result.get("htf_trend", "Neutral")
        adx = safe_float(result.get("adx", 0), 0)
        price = safe_float(result.get("price", 0), 0)
        vwap = safe_float(result.get("vwap", 0), 0)
        confidence = int(result.get("confidence", 0))
        regime = result.get("regime", "UNKNOWN")
        session_name = result.get("session", "UNKNOWN")
        entry = result.get("entry", "No Trade")

        if regime not in ["TREND DAY", "BREAKOUT DAY"]:
            return result

        strong_session = session_name in [
            "PRIME TREND WINDOW",
            "AFTERNOON BUILDUP",
            "CLOSING MOVE WINDOW",
        ]

        # Promote continuation setups that still came through as no-trade
        if signal in ["CALL", "PUT"] and entry in ["No Trade", "Wait", "Wait Breakout", "Wait Breakdown"]:
            if strong_session and adx >= 22:
                result["entry"] = "Trend Continuation Entry"
                confidence += 8

        # Confidence boost for aligned continuation
        if signal == "CALL" and trend == "Bullish" and price >= vwap and adx >= 25 and strong_session:
            confidence += 10

        if signal == "PUT" and trend == "Bearish" and price <= vwap and adx >= 25 and strong_session:
            confidence += 10

        # Very small HTF penalty only
        if signal != "NONE" and htf_trend not in ["Neutral", trend]:
            confidence -= 2

        confidence = max(min(confidence, 95), 50)
        result["confidence"] = confidence
        result["strength"] = classify_signal_strength(confidence, regime)

        notes = str(result.get("notes", "")).strip()
        extra_note = "Trend continuation logic applied"
        if extra_note not in notes:
            result["notes"] = f"{notes} | {extra_note}".strip(" |")

        return result

    except Exception as e:
        logger.warning("Trend continuation aggressive boost failed: %s", e)
        return result
if __name__ == "__main__":
    main()


