import os
import datetime
import time
import logging
import requests
import pandas as pd
import numpy as np

# ======= CONFIGURATION ========
SYMBOLS_CSV        = 'symbols_data.csv'
OUTPUT_CSV         = 'swing_trades.csv'
CACHE_DIR          = 'cache_daily'

# 5 years up to Jan 1, 2025
START_DATE         = datetime.date(2020, 1, 1)
END_DATE           = datetime.date(2025, 1, 1)
MA_PERIOD          = 44
SUPPORT_TOLERANCE   = 0.005   # 0.5% tolerance
MAX_TOUCH_DAYS      = 7

API_BASE_URL       = 'https://api.upstox.com/v3/historical-candle'
HEADERS            = {'Accept': 'application/json'}

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def ensure_cache():
    os.makedirs(CACHE_DIR, exist_ok=True)


def sanitize_key(key: str) -> str:
    return key.replace('|', '_')


def fetch_and_cache_daily(inst_key: str) -> pd.DataFrame:
    """
    Fetches daily OHLCV data for given instrument from Upstox API,
    caches the result locally to avoid repeated calls.
    Ensures data sorted in ascending date order.
    """
    ensure_cache()
    sd = START_DATE.strftime('%Y-%m-%d')
    ed = END_DATE.strftime('%Y-%m-%d')
    safe_key = sanitize_key(inst_key)
    cache_file = os.path.join(CACHE_DIR, f"{safe_key}_{sd}_{ed}.pkl")

    if os.path.exists(cache_file):
        logger.info(f"💾 Loaded cache for {inst_key}")
        df_cached = pd.read_pickle(cache_file)
        df_cached.sort_index(inplace=True)
        return df_cached

    url = f"{API_BASE_URL}/{inst_key}/days/1/{ed}/{sd}"
    logger.info(f"🔄 Fetching daily for {inst_key}: {sd} → {ed}")
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json().get('data', {}).get('candles', [])
    df = pd.DataFrame(data, columns=[
        'timestamp','open','high','low','close','volume','open_interest'
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    df = df.tz_localize(None)
    # Sort ascending: oldest → newest
    df.sort_index(inplace=True)
    df.to_pickle(cache_file)
    logger.info(f"✅ Cached {len(df)} bars for {inst_key}")
    return df


def detect_swing_trades(symbol: str, inst_key: str, df: pd.DataFrame) -> list:
    """
    Detects swing entry points based on 44MA incline + touches + breakout candle.
    """
    df = df.copy()
    df['ma44'] = df['close'].rolling(MA_PERIOD).mean()
    df.dropna(inplace=True)
    trades = []

    # iterate from MA_PERIOD + MAX_TOUCH_DAYS to end-1 for next day check
    for i in range(MA_PERIOD + MAX_TOUCH_DAYS, len(df) - 1):
        today = df.iloc[i]
        prev = df.iloc[i - 1]

        # 1) check MA incline
        if today['ma44'] <= prev['ma44']:
            continue

        # 2) check touches to MA in last MAX_TOUCH_DAYS
        touched = False
        for j in range(i - MAX_TOUCH_DAYS, i):
            cand = df.iloc[j]
            if cand['low'] <= cand['ma44'] * (1 + SUPPORT_TOLERANCE) and cand['high'] >= cand['ma44'] * (1 - SUPPORT_TOLERANCE):
                touched = True
                break
        if not touched:
            continue

        # 3) current candle is bullish and closes above MA
        body = today['close'] - today['open']
        if body <= 0 or today['close'] <= today['ma44']:
            continue

        # 4) next day breakout above today's open
        nxt = df.iloc[i + 1]
        if nxt['high'] <= today['high']*1.005:
            continue

        # define trade parameters
        entry_price = round(today['high']*1.005, 2)
        sl = min(prev['low'], today['low'])
        risk = entry_price - sl
        target = entry_price + 2 * risk

        trades.append({
            'symbol': symbol,
            'instrument_key' : inst_key,  
            'signal_date': today.name.date().isoformat(),
            'entry_date': nxt.name.date().isoformat(),
            'entry_price': round(entry_price, 2),
            'stop_loss': round(sl, 2),
            'target': round(target, 2)
        })

    logger.info(f"🔍 {symbol}: found {len(trades)} swing trades")
    return trades


def main():
    symbols = pd.read_csv(SYMBOLS_CSV)
    all_trades = []

    for idx, row in symbols.iterrows():
        symbol = row.get('tradingsymbol') or row.get('symbol')
        key = row['instrument_key']
        logger.info(f"Processing {symbol} ({idx+1}/{len(symbols)})")

        df_daily = fetch_and_cache_daily(key)
        trades = detect_swing_trades(symbol, key, df_daily)
        all_trades.extend(trades)

        # be polite to API
        time.sleep(0.2)

    if all_trades:
        pd.DataFrame(all_trades).sort_values(['symbol', 'entry_date']).to_csv(OUTPUT_CSV, index=False)
        logger.info(f"🎉 Saved {len(all_trades)} trades to {OUTPUT_CSV}")
    else:
        logger.info("😞 No trades found.")

if __name__ == '__main__':
    main()
