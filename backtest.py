import pandas as pd
import datetime
import logging
from stock_pick import fetch_and_cache_daily  # reuse your fetch logic

# ======= CONFIGURATION ========
TRADES_CSV           = 'swing_trades.csv'
FINAL_CSV            = 'final_trades.csv'
INITIAL_CAPITAL      = 2_00_000.0  # ₹4 lakh
MAX_RISK_PER_TRADE   = 4_000.0     # ₹2k per trade
LOOKAHEAD_DAYS       = 80          # days after last entry to simulate
MAX_HOLD_DAYS        = 42          # max holding days before forced exit
EXIT_NO_TARGET_DAYS  = 40          # days to exit if no target hit
SKIPS_ALREADY_HOLDED = 0
SKIPS_NO_CAP = 0
TRADES_ENTERED = 0
TRADES_EXITED = 0
TOTAL_CALLS_IN_CSV = 0


# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

# Helper: charges
def calculate_charges(buy_val, sell_val):
    stamp = 0.00015 * buy_val
    stt   = 0.001   * (sell_val + buy_val)
    dp    = 14.75
    return stamp + stt + dp

# Load & prepare trades
trades = pd.read_csv(TRADES_CSV, parse_dates=['signal_date', 'entry_date'])
TOTAL_CALLS_IN_CSV = len(trades)
trades.sort_values('entry_date', inplace=True)

# Build trading calendar
start_date = trades['entry_date'].min()
end_date   = trades['entry_date'].max() + pd.Timedelta(days=LOOKAHEAD_DAYS)
all_dates  = pd.bdate_range(start=start_date, end=end_date)

# State
capital        = INITIAL_CAPITAL
open_positions = []
completed      = []

# Price cache
_price_cache = {}
def get_price_df(inst_key):
    if inst_key not in _price_cache:
        df = fetch_and_cache_daily(inst_key)
        df.sort_index(inplace=True)
        _price_cache[inst_key] = df[['open', 'high', 'low', 'close']]
    return _price_cache[inst_key]

# Group signals by entry date
trades_by_date = trades.groupby('entry_date')

# Backtest loop
for today in all_dates:
    # 1. Check existing positions
    for pos in list(open_positions):
        df    = pos['price_df']
        if today not in df.index:
            continue
        row   = df.loc[today]
        low, high, close = row['low'], row['high'], row['close']
        exit_price = None
        exit_type  = None
        days_held  = (today.date() - pos['entry_date']).days

        # SL trigger
        if low <= pos['stop_loss']:
            exit_price, exit_type = pos['stop_loss'], 'SL'
        # First target hit
        elif not pos.get('hit') and high >= pos['target']:
            pos['hit'] = True
            # new_sl     = pos['entry_price'] + 0.6 * (pos['target'] - pos['entry_price'])
            # diff       = (pos['target'] - new_sl) * 0.8
            # pos['stop_loss'] = new_sl
            # pos['target']    = pos['target'] + diff
            logger.info(f"🎯🎯🎯 Target HIT for {pos['symbol']}")
            # Take exit after target hit
            exit_price, exit_type = pos['target'], 'TARGET HIT 🎯🎯🎯'

        # Subsequent trailing
        # elif pos.get('hit') and high >= pos['target']:
        #     trail_diff = (pos['target'] - pos['stop_loss']) * 0.25
        #     pos['stop_loss'] += trail_diff
        #     pos['target']     += trail_diff
        #     logger.info(f"↗️↗️↗️ Trailed {pos['symbol']} → SL: {pos['stop_loss']:.2f}, Target: {pos['target']:.2f}")
        # No target exit
        elif not pos.get('hit') and days_held >= EXIT_NO_TARGET_DAYS:
            exit_price, exit_type = low, 'NO_TARGET'
        # Max hold exit
        # if days_held >= MAX_HOLD_DAYS:
        #     exit_price, exit_type = close, 'MAX_HOLD'

        if exit_price is not None:
            qty       = pos['qty']
            buy_val   = pos['entry_price'] * qty
            sell_val  = exit_price * qty
            charges   = calculate_charges(buy_val, sell_val)
            pnl       = sell_val - buy_val - charges

            completed.append({
                'symbol'      : pos['symbol'],
                'entry_date'  : pos['entry_date'],
                'exit_date'   : today.date(),
                'entry_price' : pos['entry_price'],
                'exit_price'  : exit_price,
                'qty'         : qty,
                'reason'      : exit_type,
                'charges'     : round(charges, 2),
                'pnl'         : round(pnl, 2),
                'holding_days': days_held
            })
            capital += (sell_val - charges)
            open_positions.remove(pos)
            TRADES_EXITED = TRADES_EXITED +1
            logger.info(f"❌ Exited {pos['symbol']} | {exit_type} | PnL: {pnl:.2f}")

    # 2. New entry signals
    if today in trades_by_date.groups:
        for _, call in trades_by_date.get_group(today).iterrows():
            inst_key = call['instrument_key']
            symbol   = call['symbol']
            # skip if already holding
            if any(p['symbol'] == symbol for p in open_positions):
                print("SKIPPING 🚧 ALREADY HOLDED")
                SKIPS_ALREADY_HOLDED = SKIPS_ALREADY_HOLDED + 1
                continue

            entry_p = call['entry_price']
            sl      = call['stop_loss']
            target  = call['target']
            risk_sh = entry_p - sl
            qty     = int(MAX_RISK_PER_TRADE / risk_sh) if risk_sh > 0 else 0
            cost    = entry_p * qty
            if qty < 1 or cost > capital:
                print("SKIPPING ⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️ NO CAP")
                SKIPS_NO_CAP = SKIPS_NO_CAP + 1
                continue

            df_future = get_price_df(inst_key).loc[lambda d: d.index >= today]
            pos = {
                'symbol'      : symbol,
                'entry_date'  : today.date(),
                'entry_price' : entry_p,
                'stop_loss'   : sl,
                'target'      : target,
                'qty'         : qty,
                'hit'         : False,
                'price_df'    : df_future
            }
            capital -= cost
            open_positions.append(pos)
            TRADES_ENTERED = TRADES_ENTERED + 1
            logger.info(f"🟢 Entered {symbol} | qty={qty} @ {entry_p:.2f}")

# 3. Force-exit any remaining positions at the end of the backtest period
final_date = all_dates[-1]
for pos in list(open_positions):
    df   = pos['price_df']
    # Determine exit price on final_date or last available
    if final_date in df.index:
        exit_price = df.loc[final_date, 'open']
    else:
        # fallback to last available bar
        exit_price = df.iloc[-1]['open']
    qty        = pos['qty']
    buy_val    = pos['entry_price'] * qty
    sell_val   = exit_price * qty
    charges    = calculate_charges(buy_val, sell_val)
    pnl        = sell_val - buy_val - charges

    days_held  = (final_date.date() - pos['entry_date']).days
    completed.append({
        'symbol'      : pos['symbol'],
        'entry_date'  : pos['entry_date'],
        'exit_date'   : final_date.date(),
        'entry_price' : pos['entry_price'],
        'exit_price'  : exit_price,
        'qty'         : qty,
        'reason'      : 'EOD_EXIT',
        'charges'     : round(charges, 2),
        'pnl'         : round(pnl, 2),
        'holding_days': days_held
    })
    capital += (sell_val - charges)
    TRADES_EXITED = TRADES_EXITED + 1
    logger.info(f"🏁 Force-exited {pos['symbol']} @ {exit_price:.2f} | PnL: {pnl:.2f}")

# 4. Compile results & summary Compile results & summary
final_df = pd.DataFrame(completed)
final_df.to_csv(FINAL_CSV, index=False)

total   = len(final_df)
wins    = (final_df['pnl'] > 0).sum()
losses  = (final_df['pnl'] <= 0).sum()
win_pct = wins / total * 100 if total else 0
avg_pnl = final_df['pnl'].mean() if total else 0
best    = final_df['pnl'].max() if total else 0
worst   = final_df['pnl'].min() if total else 0
tot_pnl = final_df['pnl'].sum()
tot_charges = final_df['charges'].sum()
gross_profit = tot_pnl-tot_charges

print("\n🎯 Backtest Summary 🎯\n")
# 5. Year-wise % returns
final_df['year'] = pd.to_datetime(final_df['exit_date']).dt.year
yearly_returns = final_df.groupby('year')['pnl'].sum().to_dict()

print("\n📅 Year-wise % Return Summary 📅")
for yr in sorted(yearly_returns):
    yr_pnl = yearly_returns[yr]
    yr_pct = (yr_pnl / INITIAL_CAPITAL) * 100
    print(f"📆 {yr}: ₹{yr_pnl:.2f}   ➤   {yr_pct:.2f}% return")

# Overall return %
overall_pct_return = (final_df['pnl'].sum() / INITIAL_CAPITAL) * 100
print(f"\n📊 Overall Return %: {overall_pct_return:.2f}% from  ₹{INITIAL_CAPITAL:,.2f} to ₹{INITIAL_CAPITAL+gross_profit:,.2f} \n")

print(f"Total Trades      : {total}")
print(f"✅ Winners         : {wins} ({win_pct:.1f}%)")
print(f"❌ Losers          : {losses}")
print(f"Avg Holding Days   : {round(final_df['holding_days'].mean(), 2)}")
print(f"💰 Total PnL        : {tot_pnl:.2f}")
print(f"📈 Avg. PnL/Trade   : {avg_pnl:.2f}")
print(f"🏆 Best Trade PnL   : {best:.2f}")
print(f"📉 Worst Trade PnL  : {worst:.2f}")
print(f"💸 Total Charges    : {tot_charges:.2f}")
print(f"🏦 Gross Profit   : {gross_profit:.2f}   🎉 \n")

print(f"Total Skips - No cap available   : {SKIPS_NO_CAP:.2f}")
print(f"Total SKips - Already holded   : {SKIPS_ALREADY_HOLDED:.2f}")
print(f"Total Skips  : {SKIPS_ALREADY_HOLDED + SKIPS_NO_CAP:.2f}")
print(f"Total Trades Entered    : {TRADES_ENTERED:.2f}")
print(f"Total Trades Exited    : {TRADES_EXITED:.2f}   🎉")
print(f"Total Trades Call in Csv    : {TOTAL_CALLS_IN_CSV:.2f}")
print(f"Total Trades Executed + Skipped    : {TRADES_EXITED + SKIPS_ALREADY_HOLDED + SKIPS_NO_CAP:.2f}   🎉")
