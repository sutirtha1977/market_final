import os
import pandas as pd
import traceback
from datetime import datetime
from tqdm import tqdm
from db.connection import get_db_connection, close_db_connection
from config.paths import YAHOO_DIR
from config.db_table import ASSET_PRICE_SYMBOL_MAP, ASSET_TABLE_MAP
from config.nse_constants import FREQUENCIES
from config.logger import log

#################################################################################################
# Unified CSV importer for ALL asset types (PostgreSQL)
#################################################################################################
def import_csv_to_db(asset_type="india_equity"):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        if asset_type not in ASSET_TABLE_MAP:
            raise ValueError(f"Unsupported data_type: {asset_type}")

        lookup_table = ASSET_TABLE_MAP[asset_type][0]
        table_name = ASSET_TABLE_MAP[asset_type][1]

        id_col = "symbol_id"
        id_lookup_col = "yahoo_symbol"

        numeric_cols = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

        print(f"\n[CONFIG] asset_type={asset_type} | price_table={table_name} | lookup_table={lookup_table}")

        # --------------------------------------------------
        # PROCESS TIMEFRAMES
        # --------------------------------------------------
        for timeframe in FREQUENCIES:
            timeframe_path = os.path.join(YAHOO_DIR, timeframe)
            if not os.path.exists(timeframe_path):
                print(f"⚠️  No folder for timeframe '{timeframe}', skipping")
                continue

            files = [f for f in os.listdir(timeframe_path) if f.lower().endswith(".csv")]
            if not files:
                print(f"⚠️  No CSV files in {timeframe_path}, skipping")
                continue

            print(f"\n===== IMPORTING {asset_type.upper()} | {timeframe} | {len(files)} files =====")
            rows_inserted = 0

            # --------------------------------------------------
            # tqdm progress bar for CSV files
            # --------------------------------------------------
            for csv_file in tqdm(files, desc=f"{timeframe}", ncols=100):
                csv_path = os.path.join(timeframe_path, csv_file)
                symbol_name = os.path.splitext(csv_file)[0]

                # --------------------------------------------------
                # LOOKUP SYMBOL_ID
                # --------------------------------------------------
                try:
                    cur.execute(
                        f"SELECT {id_col} FROM {lookup_table} WHERE {id_lookup_col} = %s",
                        (symbol_name,)
                    )
                    res = cur.fetchone()
                    if not res:
                        log(f"❌ LOOKUP FAILED | CSV={symbol_name} | table={lookup_table} | column={id_lookup_col}")
                        continue
                    symbol_id = res[0]

                    # --------------------------------------------------
                    # READ CSV
                    # --------------------------------------------------
                    df = pd.read_csv(csv_path)
                    if df.empty:
                        log(f"⚠️ CSV EMPTY | {symbol_name}")
                        continue

                    df.columns = [c.strip() for c in df.columns]
                    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)  # <-- ROUNDING ADDED

                    # --------------------------------------------------
                    # PREPARE ROWS
                    # --------------------------------------------------
                    rows = [
                        (
                            symbol_id,
                            timeframe,
                            row["Date"],
                            row.get("Open"),
                            row.get("High"),
                            row.get("Low"),
                            row.get("Close"),
                            row.get("Adj Close"),
                            row.get("Volume"),
                        )
                        for _, row in df.iterrows()
                    ]
                    if not rows:
                        log(f"⚠️ No rows prepared | {symbol_name}")
                        continue

                    # --------------------------------------------------
                    # INSERT INTO DB
                    # --------------------------------------------------
                    insert_sql = f"""
                        INSERT INTO {table_name}
                        (symbol_id, timeframe, date, open, high, low, close, adj_close, volume)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (symbol_id, timeframe, date)
                        DO UPDATE SET
                            open      = EXCLUDED.open,
                            high      = EXCLUDED.high,
                            low       = EXCLUDED.low,
                            close     = EXCLUDED.close,
                            adj_close = EXCLUDED.adj_close,
                            volume    = EXCLUDED.volume
                    """
                    cur.executemany(insert_sql, rows)
                    rows_inserted += len(rows)

                except Exception as e:
                    log(f"❌ FAILED {symbol_name} | {timeframe} | {e}")
                    traceback.print_exc()

            # --------------------------------------------------
            # COMMIT PER TIMEFRAME
            # --------------------------------------------------
            conn.commit()
            print(f"💾 COMMIT OK | {timeframe} | rows={rows_inserted}")

        print(f"\n🎉 ALL {asset_type.upper()} CSV FILES IMPORTED INTO DATABASE")

    except Exception as e:
        log(f"❌ CRITICAL FAILURE import_csv_to_db | {e}")
        traceback.print_exc()

    finally:
        if conn:
            close_db_connection(conn)
            
#################################################################################################
# Saves a pandas DataFrame as a timestamped CSV in the scanner folder, 
# ensuring the folder exists and logging success or errors
#################################################################################################  
def export_to_csv(df: pd.DataFrame, folder: str, base_name: str) -> str:
    try:
        # Ensure folder exists
        os.makedirs(folder, exist_ok=True)

        # Generate filename with timestamp
        ts = datetime.now().strftime("%d%b%Y")
        filename = f"{base_name}_{ts}.csv"
        filepath = os.path.join(folder, filename)

        # Save CSV
        df.to_csv(filepath, index=False)
        log(f"✔ CSV saved at {filepath}")

        return os.path.abspath(filepath)

    except Exception as e:
        log(f"❌ CSV export failed | {e}")
        traceback.print_exc()
        return ""