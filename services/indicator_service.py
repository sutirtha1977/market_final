from db.connection import get_db_connection, close_db_connection
from config.logger import log
from config.nse_constants import FREQUENCIES
from config.db_table import ASSET_TABLE_MAP
from services.indicators_helper import (
    calculate_rsi_series, calculate_bollinger, 
    calculate_atr, calculate_macd, 
    calculate_supertrend, calculate_ema, calculate_wma
)
from db.sql import SQL_INSERT
import pandas as pd
import traceback
import time
import sys
from tqdm import tqdm
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

#################################################################################################
# Calculates a full set of technical indicators
#################################################################################################
def calculate_indicators(df, latest_only=False):
    try:
        df["sma_20"] = df["adj_close"].rolling(20).mean().round(2)
        df["sma_50"] = df["adj_close"].rolling(50).mean().round(2)
        df["sma_200"] = df["adj_close"].rolling(200).mean().round(2)

        df["rsi_3"] = calculate_rsi_series(df["close"], 3)
        df["rsi_9"] = calculate_rsi_series(df["close"], 9)
        df["rsi_14"] = calculate_rsi_series(df["close"], 14)

        df["ema_rsi_9_3"] = calculate_ema(df["rsi_9"], 3)
        df["wma_rsi_9_21"] = calculate_wma(df["rsi_9"], 21)

        df["bb_upper"], df["bb_middle"], df["bb_lower"] = calculate_bollinger(df["close"])
        df["atr_14"] = calculate_atr(df)
        df["supertrend"], df["supertrend_dir"] = calculate_supertrend(df)
        df["macd"], df["macd_signal"] = calculate_macd(df["close"])
        df["pct_price_change"] = df["close"].pct_change(fill_method=None).mul(100).round(2)

        if latest_only:
            return df.iloc[[-1]].reset_index(drop=True)
        return df

    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return df

#################################################################################################
# Refreshes technical indicators (PostgreSQL)
#################################################################################################
def refresh_indicators(
    asset_types=None,  # list of keys from ASSET_TABLE_MAP; if None → all
    lookback_rows=250
):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        log("🛠 Started refresh_indicators")

        asset_keys = asset_types or ASSET_TABLE_MAP.keys()
        log(f"🔑 Asset keys to process: {list(asset_keys)}")

        for asset_key in asset_keys:
            symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_key]
            col_id = "symbol_id"  # all tables use symbol_id
            log(f"\n📂 Processing asset: {asset_key}")
            log(f"   Symbol table: {symbol_table}, Price table: {price_table}, Indicator table: {indicator_table}")

            # ------------------------------
            # Load asset IDs
            # ------------------------------
            cur.execute(f"SELECT {col_id} FROM {symbol_table}")
            asset_ids = [r[0] for r in cur.fetchall()]
            log(f"   🔢 Loaded {len(asset_ids)} assets from {symbol_table}")

            insert_template = SQL_INSERT["generic"]
            insert_sql = insert_template.format(
                indicator_table=indicator_table,
                col_id=col_id
            )
            log(f"   📝 Prepared INSERT SQL for {indicator_table}")

            # ------------------------------
            # Loop timeframes
            # ------------------------------
            for timeframe in FREQUENCIES:
                log(f"\n⏳ Processing timeframe: {timeframe}")
                tf_start = time.time()
                inserted_rows = 0
                processed_assets = 0

                # tqdm progress bar for assets
                for asset_id in tqdm(asset_ids, desc=f"{asset_key} | {timeframe}", ncols=100):
                    try:
                        # 1. Last indicator date
                        cur.execute(f"""
                            SELECT MAX(date)
                            FROM {indicator_table}
                            WHERE {col_id} = %s AND timeframe = %s
                        """, (asset_id, timeframe))
                        row = cur.fetchone()
                        last_dt = row[0] if row and row[0] is not None else None

                        # 2. Fetch price data
                        if last_dt:
                            df = pd.read_sql(f"""
                                SELECT date, open, high, low, close, adj_close
                                FROM {price_table}
                                WHERE {col_id}=%s AND timeframe=%s
                                  AND date >= (
                                      SELECT date
                                      FROM {price_table}
                                      WHERE {col_id}=%s AND timeframe=%s AND date <= %s
                                      ORDER BY date DESC
                                      OFFSET {lookback_rows} LIMIT 1
                                  )
                                ORDER BY date
                            """, conn, params=(asset_id, timeframe, asset_id, timeframe, last_dt))
                        else:
                            df = pd.read_sql(f"""
                                SELECT date, open, high, low, close, adj_close
                                FROM {price_table}
                                WHERE {col_id}=%s AND timeframe=%s
                                ORDER BY date
                            """, conn, params=(asset_id, timeframe))

                        if df.empty:
                            continue

                        # 3. Calculate indicators
                        df = calculate_indicators(df, latest_only=False)

                        # 4. Keep only new rows
                        if last_dt:
                            df = df[df["date"] > last_dt]
                        if df.empty:
                            continue

                        # 5. Prepare records
                        records = [
                            (
                                asset_id, timeframe, row["date"],
                                row["sma_20"], row["sma_50"], row["sma_200"],
                                row["rsi_3"], row["rsi_9"], row["rsi_14"],
                                row["bb_upper"], row["bb_middle"], row["bb_lower"],
                                row["atr_14"], row["supertrend"], row["supertrend_dir"],
                                row["ema_rsi_9_3"], row["wma_rsi_9_21"],
                                row["pct_price_change"],
                                row["macd"], row["macd_signal"]
                            )
                            for _, row in df.iterrows()
                        ]

                        # 6. Insert
                        cur.executemany(insert_sql, records)
                        conn.commit()

                        inserted_rows += len(records)
                        processed_assets += 1

                    except Exception as e:
                        log(f"❌ ERROR {asset_key} {asset_id} {timeframe} | {e}")
                        traceback.print_exc()

                log(
                    f"  ✔ {asset_key} {timeframe} DONE | "
                    f"{processed_assets} assets | {inserted_rows} rows | "
                    f"{time.time() - tf_start:.1f}s"
                )

        log("🎉 All indicators refreshed successfully!")

    except Exception as e:
        log(f"❌ CRITICAL FAILURE — REFRESH INDICATORS | {e}")
        traceback.print_exc()

    finally:
        try:
            cur.close()
        except:
            pass
        close_db_connection(conn)