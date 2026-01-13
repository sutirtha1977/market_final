import os
import pandas as pd
import traceback
from datetime import timedelta
from db.connection import get_db_connection, close_db_connection
from services.import_export_service import export_to_csv
from config.logger import log
from config.db_table import ASSET_TABLE_MAP

LOOKBACK_DAYS = 365

#################################################################################################
# Helper: get next Monday after a given date
#################################################################################################
def next_monday(dt):
    days_ahead = 7 - dt.weekday()
    return dt + timedelta(days=days_ahead)


#################################################################################################
# WEEKLY BACKTEST: buy on signal day’s open, sell on Friday's close
#################################################################################################
def backtest_weekly_scanners(asset_type: str = "india_equity", folder_path: str = None):
    INITIAL_CAPITAL = 1_000_000
    all_trades_df = pd.DataFrame()
    all_summaries = []

    if not folder_path or not os.path.exists(folder_path):
        log(f"❌ Invalid folder path: {folder_path}")
        return pd.DataFrame()  # return empty summary

    try:
        csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        if not csv_files:
            log("❌ No scanner CSVs found")
            return pd.DataFrame()

        if asset_type not in ASSET_TABLE_MAP:
            raise ValueError(f"Unsupported asset_type: {asset_type}")

        symbol_table, price_table, _, _ = ASSET_TABLE_MAP[asset_type]

        conn = get_db_connection()
        log(f"🔍 Starting weekly backtest for {len(csv_files)} scanner files...")

        # symbol_id → yahoo_symbol & name mapping
        symbol_map = pd.read_sql(
            f"SELECT symbol_id, yahoo_symbol, name FROM {symbol_table}",
            conn
        ).set_index('symbol_id').to_dict('index')

        for file_name in csv_files:
            # print("\n" + "="*70)
            # print(f"📂 SCANNER : {file_name.replace('.csv','')}")
            # print("="*70)

            current_capital = INITIAL_CAPITAL
            weekly_capital_log = []
            trades_list = []

            try:
                path = os.path.join(folder_path, file_name)
                df_csv = pd.read_csv(path)
                if df_csv.empty:
                    log(f"⚠ Skipping {file_name} | Empty file")
                    continue

                for col in ['symbol_id', 'yahoo_symbol', 'date']:
                    if col not in df_csv.columns:
                        log(f"⚠ Skipping {file_name} | Missing column: {col}")
                        continue

                df_csv['date'] = pd.to_datetime(df_csv['date'])
                df_csv = df_csv.sort_values('date')

                # Week bucket (Monday-based)
                df_csv['week'] = df_csv['date'].dt.to_period('W-MON').apply(lambda x: x.start_time)
                last_week_start = df_csv['week'].max()

                for week_start, week_df in df_csv.groupby('week'):
                    if week_start == last_week_start:
                        continue  # skip incomplete latest week

                    signals_count = len(week_df)
                    if signals_count == 0:
                        continue

                    allocation_per_trade = current_capital / signals_count
                    weekly_pnl = 0.0

                    for _, row in week_df.iterrows():
                        symbol_id = row['symbol_id']
                        signal_date = row['date']

                        # ENTRY (signal day open)
                        entry_sql = f"""
                            SELECT date, open
                            FROM {price_table}
                            WHERE symbol_id=%s AND timeframe='1d' AND date=%s
                            ORDER BY date ASC
                            LIMIT 1
                        """
                        entry_df = pd.read_sql(entry_sql, conn,
                                               params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
                        if entry_df.empty:
                            continue

                        entry_date = pd.to_datetime(entry_df.iloc[0]['date'])
                        entry_price = entry_df.iloc[0]['open']

                        # EXIT (Friday close)
                        friday_date = entry_date + pd.offsets.Week(weekday=4)
                        exit_sql = f"""
                            SELECT date, close
                            FROM {price_table}
                            WHERE symbol_id=%s AND timeframe='1d' AND date <= %s
                            ORDER BY date DESC
                            LIMIT 1
                        """
                        exit_df = pd.read_sql(exit_sql, conn,
                                              params=(symbol_id, friday_date.strftime("%Y-%m-%d")))
                        if exit_df.empty:
                            continue

                        exit_date = pd.to_datetime(exit_df.iloc[0]['date'])
                        exit_price = exit_df.iloc[0]['close']

                        shares = allocation_per_trade / entry_price
                        trade_pnl = shares * (exit_price - entry_price)
                        trade_return_pct = (trade_pnl / allocation_per_trade) * 100
                        weekly_pnl += trade_pnl

                        symbol_name = symbol_map.get(symbol_id, {}).get('name', '')
                        yahoo_symbol = symbol_map.get(symbol_id, {}).get('yahoo_symbol', '')

                        trades_list.append({
                            "scanner": file_name.replace(".csv", ""),
                            "symbol_id": symbol_id,
                            "yahoo_symbol": yahoo_symbol,
                            "symbol_name": symbol_name,
                            "signal_date": signal_date,
                            "entry_date": entry_date,
                            "exit_date": exit_date,
                            "allocation": round(allocation_per_trade, 2),
                            "pnl": round(trade_pnl, 2),
                            "return_%": round(trade_return_pct, 2)
                        })

                    # Update capital
                    if weekly_pnl != 0:
                        current_capital += weekly_pnl
                        weekly_capital_log.append({
                            "week": week_start,
                            "signals": signals_count,
                            "weekly_pnl": round(weekly_pnl, 2),
                            "capital_end": round(current_capital, 2)
                        })

                # STATS
                trade_returns = [t['return_%'] for t in trades_list]
                if trade_returns:
                    total_trades = len(trade_returns)
                    win_pct = round(sum(1 for r in trade_returns if r > 0) / total_trades * 100, 2)
                    max_profit_pct = round(max(trade_returns), 2)
                    max_loss_pct = round(min(trade_returns), 2)
                else:
                    total_trades = 0
                    win_pct = max_profit_pct = max_loss_pct = 0.0

                final_capital = round(current_capital, 2)
                net_pnl = round(final_capital - INITIAL_CAPITAL, 2)
                total_return_pct = round((net_pnl / INITIAL_CAPITAL) * 100, 2)

                all_summaries.append({
                    "scanner": file_name.replace(".csv", ""),
                    "total_trades": total_trades,
                    "win_%": win_pct,
                    "max_profit_%": max_profit_pct,
                    "max_loss_%": max_loss_pct,
                    "final_capital": final_capital,
                    "net_pnl": net_pnl,
                    "total_return_%": total_return_pct
                })

                # Keep all trades for export
                if trades_list:
                    all_trades_df = pd.concat([all_trades_df, pd.DataFrame(trades_list)], ignore_index=True)

            except Exception as e_file:
                log(f"❌ Error processing {file_name} | {e_file}")
                traceback.print_exc()

    finally:
        if 'conn' in locals() and conn:
            close_db_connection(conn)
            log("🔒 Database connection closed")

    # EXPORT all trades to CSV
    if not all_trades_df.empty:
        export_to_csv(all_trades_df, folder_path, "all_trades_details")
        log(f"🎯 Full trade details exported | Total trades: {len(all_trades_df)}")

    # SUMMARY: print only
    summary_df = pd.DataFrame(all_summaries)
    if not summary_df.empty:
        summary_df = summary_df.sort_values("scanner")
        print("\n================= SCANNER PERFORMANCE SUMMARY =================")
        print(summary_df.to_string(index=False,
                                   columns=[
                                       "scanner", "total_trades", "win_%",
                                       "max_profit_%", "max_loss_%",
                                       "final_capital", "net_pnl", "total_return_%"
                                   ]))
        print("===============================================================\n")

    # Return summary only (prevents printing thousands of trades)
    return summary_df


#################################################################################################
# DAILY BACKTEST: buy next day after signal, sell after 5 trading days
#################################################################################################
def backtest_daily_scanners(asset_type: str = "india_equity", folder_path: str = None):
    all_trades = []
    all_summaries = []

    if not folder_path or not os.path.exists(folder_path):
        log(f"❌ Invalid folder path: {folder_path}")
        return pd.DataFrame(), pd.DataFrame()

    try:
        csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        if not csv_files:
            log("❌ No scanner CSVs found")
            return pd.DataFrame(), pd.DataFrame()

        if asset_type not in ASSET_TABLE_MAP:
            raise ValueError(f"Unsupported asset_type: {asset_type}")

        symbol_table, price_table, _, _ = ASSET_TABLE_MAP[asset_type]

        conn = get_db_connection()
        log(f"🔍 Starting daily backtest for {len(csv_files)} scanner files...")

        symbol_map = pd.read_sql(
            f"SELECT symbol_id, yahoo_symbol, name FROM {symbol_table}",
            conn
        ).set_index("symbol_id").to_dict("index")

        for file_name in csv_files:
            print("\n" + "=" * 70)
            print(f"📂 SCANNER : {file_name.replace('.csv','')}")
            print("=" * 70)

            try:
                path = os.path.join(folder_path, file_name)
                df_csv = pd.read_csv(path)
                if df_csv.empty:
                    log(f"⚠ Skipping {file_name} | Empty file")
                    continue

                for col in ['symbol_id', 'yahoo_symbol', 'date']:
                    if col not in df_csv.columns:
                        log(f"⚠ Skipping {file_name} | Missing column: {col}")
                        continue

                df_csv['date'] = pd.to_datetime(df_csv['date'])
                df_csv = df_csv.sort_values('date')

                for _, row in df_csv.iterrows():
                    symbol_id   = row['symbol_id']
                    signal_date = row['date']

                    entry_sql = f"""
                        SELECT date, open
                        FROM {price_table}
                        WHERE symbol_id=%s AND timeframe='1d' AND date > %s
                        ORDER BY date ASC
                        LIMIT 1
                    """
                    entry_df = pd.read_sql(entry_sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
                    if entry_df.empty:
                        continue

                    entry_date  = pd.to_datetime(entry_df.iloc[0]['date'])
                    entry_price = entry_df.iloc[0]['open']

                    exit_sql = f"""
                        SELECT date, close
                        FROM {price_table}
                        WHERE symbol_id=%s AND timeframe='1d' AND date > %s
                        ORDER BY date ASC
                        LIMIT 1 OFFSET 4
                    """
                    exit_df = pd.read_sql(exit_sql, conn, params=(symbol_id, entry_date.strftime("%Y-%m-%d")))
                    if exit_df.empty:
                        continue

                    exit_date  = pd.to_datetime(exit_df.iloc[0]['date'])
                    exit_price = exit_df.iloc[0]['close']

                    trade_return_pct = ((exit_price - entry_price) / entry_price) * 100
                    symbol_name = symbol_map.get(symbol_id, {}).get("name", "")
                    yahoo_symbol = symbol_map.get(symbol_id, {}).get("yahoo_symbol", "")

                    all_trades.append({
                        "scanner": file_name.replace(".csv", ""),
                        "symbol_id": symbol_id,
                        "yahoo_symbol": yahoo_symbol,
                        "symbol_name": symbol_name,
                        "signal_date": signal_date,
                        "entry_date": entry_date,
                        "exit_date": exit_date,
                        "entry_price": round(entry_price, 2),
                        "exit_price": round(exit_price, 2),
                        "return_%": round(trade_return_pct, 2)
                    })

                trade_returns = [t['return_%'] for t in all_trades if t['scanner'] == file_name.replace(".csv", "")]
                if trade_returns:
                    total_trades = len(trade_returns)
                    win_pct = round(sum(1 for r in trade_returns if r > 0) / total_trades * 100, 2)
                    max_profit_pct = round(max(trade_returns), 2)
                    max_loss_pct   = round(min(trade_returns), 2)
                else:
                    total_trades = 0
                    win_pct = max_profit_pct = max_loss_pct = 0.0

                all_summaries.append({
                    "scanner": file_name.replace(".csv", ""),
                    "total_trades": total_trades,
                    "win_%": win_pct,
                    "max_profit_%": max_profit_pct,
                    "max_loss_%": max_loss_pct
                })

            except Exception as e_file:
                log(f"❌ Error processing {file_name} | {e_file}")
                traceback.print_exc()

    finally:
        if 'conn' in locals() and conn:
            close_db_connection(conn)
            log("🔒 Database connection closed")

    trades_df = pd.DataFrame(all_trades)
    if not trades_df.empty:
        export_to_csv(trades_df, folder_path, "fixed_5day_trades")
        log(f"🎯 Trades exported | Total trades: {len(trades_df)}")

    summary_df = pd.DataFrame(all_summaries)
    if not summary_df.empty:
        summary_df = summary_df.sort_values("scanner")
        print("\n================= SCANNER PERFORMANCE SUMMARY =================")
        print(summary_df.to_string(index=False))
        print("===============================================================\n")

    return trades_df, summary_df