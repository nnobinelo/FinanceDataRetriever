from pathlib import Path
import MetaTrader5 as mt5

PACKAGE_ROOT_DIR = Path(__file__).parent.resolve()
PACKAGE_TEMP_DIR = PACKAGE_ROOT_DIR / "PackageData/.temp"
FOREX_DATA_DIR = "./forex_data"
LEAN_FMT_MT5_DATA_DIR = "./lean_fmt_mt5_data"
LEAN_FOREX_DATA_DIR_PATH = "data/forex/fxcm"

CONFIG_FILEPATH = "./fdr-config.json"
MT5_TERMINAL_PATH = "mt5_terminal_path"
MT5_TRADE_SERVER = "mt5_trade_server"
MT5_LOGIN_ID = "mt5_login_id"
MT5_LOGIN_PASSWORD = "mt5_login_password"
MT5_TIME_COL_NAME = "time"
MT5_OPEN_COL_NAME = "open"
MT5_HIGH_COL_NAME = "high"
MT5_LOW_COL_NAME = "low"
MT5_CLOSE_COL_NAME = "close"
MT5_TICK_VOLUME_COL_NAME = "tick_volume"
MT5_SPREAD_COL_NAME = "spread"
MT5_REAL_VOLUME_COL_NAME = "real_volume"
MT5_BID_COL_NAME = "bid"
MT5_ASK_COL_NAME = "ask"
MT5_TICK_TIMEFRAME_NAME = "tick"

MT5_TIMEFRAMES = {
    MT5_TICK_TIMEFRAME_NAME: None,
    "m1": mt5.TIMEFRAME_M1,
    "m2": mt5.TIMEFRAME_M2,
    "m3": mt5.TIMEFRAME_M3,
    "m4": mt5.TIMEFRAME_M4,
    "m5": mt5.TIMEFRAME_M5,
    "m6": mt5.TIMEFRAME_M6,
    "m10": mt5.TIMEFRAME_M10,
    "m12": mt5.TIMEFRAME_M12,
    "m15": mt5.TIMEFRAME_M15,
    "m20": mt5.TIMEFRAME_M20,
    "m30": mt5.TIMEFRAME_M30,
    "h1": mt5.TIMEFRAME_H1,
    "h2": mt5.TIMEFRAME_H2,
    "h3": mt5.TIMEFRAME_H3,
    "h4": mt5.TIMEFRAME_H4,
    "h6": mt5.TIMEFRAME_H6,
    "h8": mt5.TIMEFRAME_H8,
    "h12": mt5.TIMEFRAME_H12,
    "d1": mt5.TIMEFRAME_D1,
    "w1": mt5.TIMEFRAME_W1,
    "mn1": mt5.TIMEFRAME_MN1,
}
