import pandas as pd
import datetime
import logging
from stock_pick import fetch_and_cache_daily  # reuse your fetch logic

# ======= CONFIGURATION ========
TRADES_CSV         = 'swing_trades.csv'
FINAL_CSV          = 'final_trades.csv'
INITIAL_CAPITAL    = 400_000.0  # ₹4 lakh
MAX_RISK_PER_TRADE = 2_000.0     # ₹2k per trade
LOOKAHEAD_DAYS     = 27          # days after last entry to simulate
MAX_HOLD_DAYS      = 18          # max holding days before forced exit

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

def calculate_charges(buy_val, sell_val):
    """Stamp + STT + DP fees."""
    stamp = 0.00015 * buy_val
    stt   = 0.001   * (sell_val + buy_val)
    dp    = 14.75
    return stamp + stt + dp

# 1) Load & sort trades
trades = pd.read_csv(TRADES_CSV, parse_dates=['signal_date', 'entry_date'])
trades.sort_values('entry_date', inplace=True)

# 2) Date range: first entry → last entry + lookahead
start_date = trades['entry_date'].min()
end_date   = trades['entry_date'].max() + pd.Timedelta(days=LOOKAHEAD_DAYS)
all_dates  = pd.bdate_range(start=start_date, end=end_date)

# State
capital         = INITIAL_CAPITAL
open_positions  = []  # list of dicts
completed       = []  # list for result

# Price cache helper
_price_cache = {}
def get_price_df(inst_key):
    if inst_key not in _price_cache:
        df = fetch_and_cache_daily(inst_key)
        df.sort_index(inplace=True)
        _price_cache[inst_key] = df[['open', 'high', 'low', 'close']]
    return _price_cache[inst_key]

# Group trades by entry_date
trades_by_date = trades.groupby('entry_date')

# 🏁 Backtest loop
for today in all_dates:
    # 🛑 Check SL/Target/MaxHold for each open position
    for pos in open_positions[:]:
        df = pos['price_df']
        if today not in df.index:
            continue
        row = df.loc[today]
        exit_price = None
        reason     = None

        # SL hit
        if row['low'] <= pos['stop_loss']:
            exit_price, reason = pos['stop_loss'], 'SL'
        # Target hit
        elif row['high'] >= pos['target']:
            exit_price, reason = pos['target'], 'Target'
        # Max hold duration
        days_held = (today.date() - pos['entry_date']).days
        if exit_price is None and days_held >= MAX_HOLD_DAYS:
            exit_price, reason = row['open'], 'MaxHold'

        if exit_price is not None:
            qty      = pos['qty']
            buy_val  = pos['entry_price'] * qty
            sell_val = exit_price * qty
            charges  = calculate_charges(buy_val, sell_val)
            pnl      = sell_val - buy_val - charges

            completed.append({
                'symbol'      : pos['symbol'],
                'entry_date'  : pos['entry_date'],
                'exit_date'   : today.date(),
                'entry_price' : pos['entry_price'],
                'exit_price'  : exit_price,
                'qty'         : qty,
                'reason'      : reason,
                'charges'     : round(charges, 2),
                'pnl'         : round(pnl, 2),
                'holding_days': days_held
            })
            capital += (sell_val - charges)
            open_positions.remove(pos)
            logger.info(f"✅ Exited {pos['symbol']} | {reason} | PnL: {pnl:.2f}")

    # ➡️ New entries today
    if today in trades_by_date.groups:
        for _, call in trades_by_date.get_group(today).iterrows():
            inst_key    = call['instrument_key']
            symbol      = call['symbol']
            # Skip if already holding this symbol
            if any(p['symbol'] == symbol for p in open_positions):
                logger.info(f"🔁🔁🔁 Already holding {symbol}, skipping new entry on {today.date()}")
                continue
            entry_price = call['entry_price']
            sl          = call['stop_loss']
            target      = call['target']
            risk_share  = entry_price - sl
            qty         = int(MAX_RISK_PER_TRADE / risk_share) if risk_share > 0 else 0
            cost        = entry_price * qty
            if qty < 1 or cost > capital:
                logger.info(f"❌❌❌ Skip {symbol} on {today.date()} (qty={qty}, cost={cost:.0f}, cap={capital:.0f})")
                continue

            df_future = get_price_df(inst_key).loc[lambda d: d.index >= today]
            pos = {
                'symbol'      : symbol,
                'entry_date'  : today.date(),
                'entry_price' : entry_price,
                'stop_loss'   : sl,
                'target'      : target,
                'qty'         : qty,
                'price_df'    : df_future
            }
            capital -= cost
            open_positions.append(pos)
            logger.info(f"🟢 Entered {symbol} | qty={qty} | @ {entry_price:.2f}")

# 🚨 Force-exit remaining positions at last close
for pos in open_positions:
    df         = pos['price_df']
    last_dt    = df.index.max()
    exit_price = df.loc[last_dt, 'close']
    qty        = pos['qty']
    buy_val    = pos['entry_price'] * qty
    sell_val   = exit_price * qty
    charges    = calculate_charges(buy_val, sell_val)
    pnl        = sell_val - buy_val - charges
    days_held  = (last_dt.date() - pos['entry_date']).days

    completed.append({
        'symbol'      : pos['symbol'],
        'entry_date'  : pos['entry_date'],
        'exit_date'   : last_dt.date(),
        'entry_price' : pos['entry_price'],
        'exit_price'  : exit_price,
        'qty'         : qty,
        'reason'      : 'EOD Exit',
        'charges'     : round(charges, 2),
        'pnl'         : round(pnl, 2),
        'holding_days': days_held
    })
    capital += (sell_val - charges)
    logger.info(f"🔔 Force-exited {pos['symbol']} | PnL: {pnl:.2f}")

# 📝 Save and Detailed Summary
final_df = pd.DataFrame(completed)
final_df.to_csv(FINAL_CSV, index=False)

# Summary metrics
total      = len(final_df)
wins       = sum(final_df['pnl'] > 0)
losses     = sum(final_df['pnl'] <= 0)
win_pct    = (wins / total * 100) if total else 0
avg_pnl    = final_df['pnl'].mean() if total else 0
best       = final_df['pnl'].max() if total else 0
worst      = final_df['pnl'].min() if total else 0

print("\n🎯 Backtest Summary 🎯")
print(f"Total Trades      : {total}")
print(f"✅ Winners         : {wins} ({win_pct:.1f}%)")
print(f"❌ Losers          : {losses}")
print(f"💰 Total PnL        : {final_df['pnl'].sum():.2f}")
print(f"📈 Avg. PnL/Trade   : {avg_pnl:.2f}")
print(f"🏆 Best Trade PnL   : {best:.2f}")
print(f"📉 Worst Trade PnL  : {worst:.2f}")
print(f"💸 Total Charges    : {final_df['charges'].sum():.2f}")
print(f"🏦 Gross profit   : {final_df['pnl'].sum()-final_df['charges'].sum():.2f}   🎉")