import pandas as pd
import traceback
from datetime import datetime, timedelta
from db.connection import get_db_connection, close_db_connection
from config.logger import log
from config.db_table import ASSET_TABLE_MAP

LOOKBACK_DAYS = 365

#################################################################################################
# Fetches OHLC price and technical indicators for all symbols over 
# the specified lookback period, merging daily, weekly, and monthly indicator values
#################################################################################################
def get_base_data(
    start_date: str | None = None, 
    end_date: str | None = None, 
    asset_type: str = "india_equity"
) -> pd.DataFrame:

    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
    conn = get_db_connection()
    df_daily = pd.DataFrame()

    try:
        # ---------------------------------------------------
        # DAILY data (price + indicators)
        # ---------------------------------------------------
        daily_sql = f"""
            SELECT d.symbol_id, s.yahoo_symbol, d.date,
                p.open, p.high, p.low, p.close, p.volume, p.adj_close,
                d.pct_price_change, 
                d.rsi_3, d.rsi_9, d.rsi_14, 
                d.ema_rsi_9_3, d.wma_rsi_9_21,
                d.sma_20, d.sma_50, d.sma_200
            FROM {indicator_table} d
            JOIN {price_table} p
              ON p.symbol_id = d.symbol_id
             AND p.date = d.date
             AND p.timeframe = '1d'
            JOIN {symbol_table} s
              ON s.symbol_id = d.symbol_id
            WHERE d.timeframe = '1d'
              AND d.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY d.symbol_id, d.date
        """

        df_daily = pd.read_sql(daily_sql, conn)
        if df_daily.empty:
            print("❌ No daily data found")
            return df_daily

        df_daily['date'] = pd.to_datetime(df_daily['date'])
        print(f"📦 DAILY ROWS: {len(df_daily)}")

        # ---------------------------------------------------
        # Numeric conversion
        # ---------------------------------------------------
        numeric_cols = [
            'open','high','low','close','adj_close','volume',
            'pct_price_change',
            'rsi_3','rsi_9','rsi_14','ema_rsi_9_3','wma_rsi_9_21',
            'sma_20','sma_50','sma_200'
        ]
        for col in numeric_cols:
            df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')

        # ---------------------------------------------------
        # WEEKLY indicators
        # ---------------------------------------------------
        weekly_sql = f"""
            SELECT
                symbol_id,
                date AS weekly_date,
                rsi_3 AS rsi_3_weekly,
                rsi_9 AS rsi_9_weekly,
                rsi_14 AS rsi_14_weekly,
                ema_rsi_9_3 AS ema_rsi_9_3_weekly,
                wma_rsi_9_21 AS wma_rsi_9_21_weekly
            FROM {indicator_table}
            WHERE timeframe = '1wk'
              AND date BETWEEN '{start_date}' AND '{end_date}'
        """
        df_weekly = pd.read_sql(weekly_sql, conn)
        df_weekly['weekly_date'] = pd.to_datetime(df_weekly['weekly_date'])

        df_daily = df_daily.merge(df_weekly, on='symbol_id', how='left')
        df_daily = df_daily[df_daily['weekly_date'] <= df_daily['date']]
        df_daily = (
            df_daily
            .sort_values(['symbol_id','date','weekly_date'])
            .groupby(['symbol_id','date'], as_index=False)
            .last()
        )

        # ---------------------------------------------------
        # MONTHLY indicators
        # ---------------------------------------------------
        monthly_sql = f"""
            SELECT
                symbol_id,
                date AS monthly_date,
                rsi_3 AS rsi_3_monthly,
                rsi_9 AS rsi_9_monthly,
                rsi_14 AS rsi_14_monthly,
                ema_rsi_9_3 AS ema_rsi_9_3_monthly,
                wma_rsi_9_21 AS wma_rsi_9_21_monthly
            FROM {indicator_table}
            WHERE timeframe = '1mo'
              AND date BETWEEN '{start_date}' AND '{end_date}'
        """
        df_monthly = pd.read_sql(monthly_sql, conn)
        df_monthly['monthly_date'] = pd.to_datetime(df_monthly['monthly_date'])

        df_daily = df_daily.merge(df_monthly, on='symbol_id', how='left')
        df_daily = df_daily[df_daily['monthly_date'] <= df_daily['date']]
        df_daily = (
            df_daily
            .sort_values(['symbol_id','date','monthly_date'])
            .groupby(['symbol_id','date'], as_index=False)
            .last()
        )

        print(f"✅ FINAL BASE DATA ROWS: {len(df_daily)}")
        return df_daily

    except Exception as e:
        print(f"❌ get_base_data FAILED | {e}")
        traceback.print_exc()
        return df_daily

    finally:
        close_db_connection(conn)
#################################################################################################
# This function pulls weekly stock data and indicators, computes trend and momentum 
# conditions (SMA slope, pullback to recent lows, and improving closes), and returns 
# only those weeks where the stock shows a bullish continuation setup
#################################################################################################
def get_base_data_weekly(
    asset_type: str = "india_equity", 
    start_date: str | None = None, 
    end_date: str | None = None
) -> pd.DataFrame:

    conn = get_db_connection()
    df_weekly = pd.DataFrame()

    try:
        print("🔍 FETCHING WEEKLY DATA...")

        if asset_type not in ASSET_TABLE_MAP:
            raise ValueError(f"Unsupported asset_type: {asset_type}")

        # -----------------------------
        # Get tables from ASSET_TABLE_MAP
        # -----------------------------
        symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
        id_col = "symbol_id"

        # -----------------------------
        # Default date range
        # -----------------------------
        start_date = start_date or "2000-01-01"
        end_date = end_date or "2099-12-31"

        # -----------------------------
        # SQL query
        # -----------------------------
        sql = f"""
            WITH weekly_price AS (
                SELECT
                    ep.{id_col},
                    ep.date,
                    ep.open,
                    ep.high,
                    ep.low,
                    ep.close,
                    AVG(ep.close) OVER (
                        PARTITION BY ep.{id_col} 
                        ORDER BY ep.date 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20
                FROM {price_table} ep
                WHERE ep.timeframe = '1wk'
                AND ep.date BETWEEN '{start_date}' AND '{end_date}'
            ),

            weekly_with_lags AS (
                SELECT
                    wp.*,
                    LAG(wp.close, 1) OVER (
                        PARTITION BY wp.{id_col} 
                        ORDER BY wp.date
                    ) AS close_1w_ago,

                    LAG(wp.sma_20, 2) OVER (
                        PARTITION BY wp.{id_col} 
                        ORDER BY wp.date
                    ) AS sma_20_2w_ago,

                    MIN(wp.low) OVER (
                        PARTITION BY wp.{id_col} 
                        ORDER BY wp.date 
                        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                    ) AS min_low_4w
                FROM weekly_price wp
            ),

            weekly_indicators AS (
                SELECT
                    wi.{id_col},
                    wi.date,
                    wi.rsi_3,
                    wi.rsi_9,
                    wi.rsi_14,
                    wi.ema_rsi_9_3,
                    wi.wma_rsi_9_21
                FROM {indicator_table} wi
                WHERE wi.timeframe = '1wk'
                AND wi.date BETWEEN '{start_date}' AND '{end_date}'
            )

            SELECT
                p.{id_col},
                s.yahoo_symbol,
                s.name,
                p.date,

                p.open,
                p.high,
                p.low,
                p.close,

                p.sma_20,
                p.sma_20_2w_ago,
                p.close_1w_ago,
                p.min_low_4w,

                i.rsi_3,
                i.rsi_9,
                i.rsi_14,
                i.ema_rsi_9_3,
                i.wma_rsi_9_21

            FROM weekly_with_lags p
            JOIN weekly_indicators i
            ON p.{id_col} = i.{id_col}
            AND p.date = i.date
            JOIN {symbol_table} s
            ON p.{id_col} = s.{id_col}

            WHERE p.close > p.sma_20
            AND p.low <= p.min_low_4w
            AND p.sma_20_2w_ago < p.sma_20
            AND p.close >= p.close_1w_ago

            ORDER BY p.{id_col}, p.date;
        """

        log(sql)
        df_weekly = pd.read_sql(sql, conn)

        return df_weekly

    except Exception as e:
        print(f"❌ get_base_data_weekly FAILED | {e}")
        traceback.print_exc()
        return df_weekly

    finally:
        close_db_connection(conn)
#################################################################################################
# This function builds a unified dataset that aligns daily prices with the latest weekly 
# and monthly data, adding Bollinger upper bands and previous closes for each timeframe 
# so you can detect crossovers in daily, weekly, and monthly charts in one scan.
#################################################################################################  
# def get_base_data_with_prev_candle(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:

#     conn = get_db_connection()
#     df_daily = pd.DataFrame()

#     try:

#         print("🔍 FETCHING BASE DATA (Daily + Weekly + Monthly)...")

#         # ===================================================
#         # 1️⃣ DAILY : close + bb_upper
#         # ===================================================
#         daily_sql = f"""
#             SELECT
#                 p.symbol_id,
#                 s.yahoo_symbol,
#                 p.date,
#                 p.close,
#                 i.bb_upper,
#                 i.rsi_3 as rsi_3_d,
#                 i.rsi_9 AS rsi_9_d,
#                 i.ema_rsi_9_3 as ema_rsi_9_3_d,
#                 i.wma_rsi_9_21 as wma_rsi_9_21_d
#             FROM india_equity_price_data p
#             JOIN india_equity_indicators i
#               ON i.symbol_id = p.symbol_id
#              AND i.date      = p.date
#              AND i.timeframe = '1d'
#             JOIN india_equity_symbols s
#               ON s.symbol_id = p.symbol_id
#             WHERE p.timeframe = '1d'
#               AND p.date BETWEEN '{start_date}' AND '{end_date}'
#             ORDER BY p.symbol_id, p.date
#         """

#         df_daily = pd.read_sql(daily_sql, conn)
#         if df_daily.empty:
#             print("❌ No daily data found")
#             return df_daily

#         df_daily['date'] = pd.to_datetime(df_daily['date'])

#         # ---- previous daily close
#         df_daily['close_prev'] = (
#             df_daily
#             .sort_values(['symbol_id','date'])
#             .groupby('symbol_id')['close']
#             .shift(1)
#         )

#         # ===================================================
#         # 2️⃣ WEEKLY : close + bb_upper
#         # ===================================================
#         weekly_sql = f"""
#             SELECT
#                 p.symbol_id,
#                 p.date AS weekly_date,
#                 i.rsi_3 as rsi_3_w,
#                 i.rsi_9 AS rsi_9_w,
#                 i.ema_rsi_9_3 as ema_rsi_9_3_w,
#                 i.wma_rsi_9_21 as wma_rsi_9_21_w,
#                 p.close AS close_weekly,
#                 i.bb_upper AS bb_upper_weekly
#             FROM india_equity_price_data p
#             JOIN india_equity_indicators i
#               ON i.symbol_id = p.symbol_id
#              AND i.date      = p.date
#              AND i.timeframe = '1wk'
#             WHERE p.timeframe = '1wk'
#               AND p.date BETWEEN '{start_date}' AND '{end_date}'
#         """

#         df_weekly = pd.read_sql(weekly_sql, conn)
#         df_weekly['weekly_date'] = pd.to_datetime(df_weekly['weekly_date'])

#         # ---- previous weekly close
#         df_weekly['close_weekly_prev'] = (
#             df_weekly
#             .sort_values(['symbol_id','weekly_date'])
#             .groupby('symbol_id')['close_weekly']
#             .shift(1)
#         )

#         # ---- merge weekly → daily (latest weekly ≤ daily)
#         df_daily = df_daily.merge(df_weekly, on='symbol_id', how='left')
#         df_daily = df_daily[df_daily['weekly_date'] <= df_daily['date']]
#         df_daily = (
#             df_daily
#             .sort_values(['symbol_id','date','weekly_date'])
#             .groupby(['symbol_id','date'], as_index=False)
#             .last()
#         )

#         # ===================================================
#         # 3️⃣ MONTHLY : close + bb_upper
#         # ===================================================
#         monthly_sql = f"""
#             SELECT
#                 p.symbol_id,
#                 p.date AS monthly_date,
#                 p.close AS close_monthly,
#                 i.bb_upper AS bb_upper_monthly,
#                 i.rsi_3 as rsi_3_m,
#                 i.rsi_9 AS rsi_9_m,
#                 i.ema_rsi_9_3 as ema_rsi_9_3_m,
#                 i.wma_rsi_9_21 as wma_rsi_9_21_m
#             FROM india_equity_price_data p
#             JOIN india_equity_indicators i
#               ON i.symbol_id = p.symbol_id
#              AND i.date      = p.date
#              AND i.timeframe = '1mo'
#             WHERE p.timeframe = '1mo'
#               AND p.date BETWEEN '{start_date}' AND '{end_date}'
#         """

#         df_monthly = pd.read_sql(monthly_sql, conn)
#         df_monthly['monthly_date'] = pd.to_datetime(df_monthly['monthly_date'])

#         # ---- previous monthly close
#         df_monthly['close_monthly_prev'] = (
#             df_monthly
#             .sort_values(['symbol_id','monthly_date'])
#             .groupby('symbol_id')['close_monthly']
#             .shift(1)
#         )

#         # ---- merge monthly → daily (latest monthly ≤ daily)
#         df_daily = df_daily.merge(df_monthly, on='symbol_id', how='left')
#         df_daily = df_daily[df_daily['monthly_date'] <= df_daily['date']]
#         df_daily = (
#             df_daily
#             .sort_values(['symbol_id','date','monthly_date'])
#             .groupby(['symbol_id','date'], as_index=False)
#             .last()
#         )

#         # ===================================================
#         # FINAL COLUMNS (only what scanner needs)
#         # ===================================================
#         final_cols = [
#             'symbol_id','symbol','date',

#             # Daily
#             'close','bb_upper','close_prev', 
#             "rsi_3_d","rsi_9_d","ema_rsi_9_3_d","wma_rsi_9_21_d",

#             # Weekly
#             'close_weekly','bb_upper_weekly','close_weekly_prev',
#             "rsi_3_w","rsi_9_w","ema_rsi_9_3_w","wma_rsi_9_21_w",

#             # Monthly
#             'close_monthly','bb_upper_monthly','close_monthly_prev',
#             "rsi_3_m","rsi_9_m","ema_rsi_9_3_m","wma_rsi_9_21_m"
#         ]

#         df_daily = df_daily[final_cols]

#         print(f"✅ FINAL BASE DATA ROWS: {len(df_daily)}")

#         return df_daily

#     except Exception as e:
#         print(f"❌ get_base_data FAILED | {e}")
#         traceback.print_exc()
#         return df_daily

#     finally:
#         close_db_connection(conn)
        
#################################################################################################
# Retrieves OHLC price and indicator data for a single symbol and 
# timeframe over a given lookback period.
#################################################################################################
# def fetch_price_data_for_symbol_timeframe(conn, symbol_id: int, timeframe: str, lookback_days=LOOKBACK_DAYS):
#     """
#     Fetch OHLCV + indicators for a given symbol and timeframe.
#     """
#     from datetime import datetime, timedelta
#     import pandas as pd

#     end_date = datetime.today().date()
#     start_date = end_date - timedelta(days=lookback_days)

#     sql = """
#         SELECT p.date, p.open, p.high, p.low, p.close, p.volume,
#                p.adj_close, d.rsi_3, d.rsi_9, d.ema_rsi_9_3, d.wma_rsi_9_21,
#                d.sma_20, d.sma_50, d.sma_200, d.pct_price_change
#         FROM india_equity_price_data p
#         LEFT JOIN india_equity_indicators d
#           ON p.symbol_id = d.symbol_id AND p.date = d.date AND d.timeframe = ?
#         WHERE p.symbol_id = ? AND p.timeframe = ? AND p.date >= ?
#         ORDER BY p.date ASC
#     """
#     df = pd.read_sql(sql, conn, params=(timeframe, symbol_id, timeframe, start_date))
#     if not df.empty:
#         df['date'] = pd.to_datetime(df['date'])
#     return df
#################################################################################################
# Identifies the candlestick pattern (Doji, Hammer, Shooting Star, 
# Marubozu, Bullish/Bearish) based on OHLC data.
#################################################################################################   
def get_candle_type(open, high, low, close):
    body = abs(close - open)
    upper_shadow = high - max(open, close)
    lower_shadow = min(open, close) - low

    if high == low:
        return "Doji"

    if close > open:
        color = "Bullish"
    elif close < open:
        color = "Bearish"
    else:
        return "Doji"

    if body < 0.1 * (high - low):
        return "Doji"

    if body <= (high - low) * 0.3 and lower_shadow >= 2 * body and upper_shadow <= 0.3 * body:
        return "Hammer" if color == "Bullish" else "Hanging Man"

    if body <= (high - low) * 0.3 and upper_shadow >= 2 * body and lower_shadow <= 0.3 * body:
        return "Inverted Hammer" if color == "Bullish" else "Shooting Star"

    if upper_shadow < 0.05 * body and lower_shadow < 0.05 * body:
        return f"{color} Marubozu"

    return color