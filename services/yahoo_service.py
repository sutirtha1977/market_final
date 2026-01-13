import os
import traceback
from datetime import datetime, date, timedelta
import yfinance as yf
import pandas as pd
from tqdm import tqdm
from config.logger import log
from config.paths import YAHOO_DIR
from config.nse_constants import FREQUENCIES
from db.connection import get_db_connection, close_db_connection
from config.db_table import ASSET_TABLE_MAP  # use this

SKIP_MONTHLY = (date.today().day != 1)
today_weekday = datetime.now().weekday()
SKIP_WEEKLY = today_weekday != 0   # Monday = 0


# ============================================================
# Unified Yahoo Downloader for ALL asset types (DEBUG VERSION)
# ============================================================
def download_yahoo_data_all_timeframes(
    asset_type,
    symbols="ALL",          # "ALL" or "AAPL,MSFT"
    mode="full",           # "full" | "incr"
    latest_dt=None         # required if mode="incr"
):
    conn = None
    failed_symbols = []  # Track all failures

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        log(f"🚀 START DOWNLOAD | asset_type={asset_type} | symbols={symbols} | mode={mode}")

        # -------------------------------
        # SYMBOL TABLE FROM ASSET_TABLE_MAP
        # -------------------------------
        if asset_type not in ASSET_TABLE_MAP:
            raise ValueError(f"Unsupported asset_type: {asset_type}")

        symbol_table = ASSET_TABLE_MAP[asset_type][0]  # first element = symbols table
        log(f"📘 Using symbol table from ASSET_TABLE_MAP: {symbol_table}")

        # -------------------------------
        # HANDLE MODE / DATES
        # -------------------------------
        if mode not in ("full", "incr"):
            raise ValueError("mode must be 'full' or 'incr'")

        if mode == "incr":
            if not latest_dt:
                raise ValueError("latest_dt is required for incremental mode")
            start_date = pd.to_datetime(latest_dt).date() + timedelta(days=1)
            end_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
            log(f"Incremental download from {start_date} → {end_date}")

        # -------------------------------
        # FETCH SYMBOLS
        # -------------------------------
        if symbols == "ALL":
            cur.execute(f"SELECT symbol_id, yahoo_symbol FROM {symbol_table} WHERE is_active = TRUE")
        else:
            symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
            if not symbol_list:
                log("❌ After cleaning, symbol_list is EMPTY")
                return

            placeholders = ",".join(["%s"] * len(symbol_list))
            cur.execute(
                f"SELECT symbol_id, yahoo_symbol FROM {symbol_table} "
                f"WHERE yahoo_symbol IN ({placeholders}) AND is_active = TRUE",
                symbol_list
            )

        rows = cur.fetchall()
        if not rows:
            log(f"No symbols found in {symbol_table} for {symbols}")
            return

        log(f"📌 {len(rows)} symbols loaded for {asset_type}")

        # -------------------------------
        # PROCESS ALL TIMEFRAMES
        # -------------------------------
        for timeframe in FREQUENCIES:
            timeframe_path = os.path.join(YAHOO_DIR, timeframe)
            os.makedirs(timeframe_path, exist_ok=True)

            print(f"\nDownloading {asset_type.upper()} | timeframe: {timeframe}")

            for symbol_id, yahoo_symbol in tqdm(rows, desc=f"{timeframe}", ncols=100):
                download_symbol = yahoo_symbol

                # NSE adjustment
                if asset_type == "india_equity" and not yahoo_symbol.endswith(".NS"):
                    download_symbol = f"{yahoo_symbol}.NS"

                csv_path = os.path.join(timeframe_path, f"{yahoo_symbol}.csv")

                try:
                    if mode == "full":
                        df = yf.download(
                            download_symbol,
                            period="max",
                            interval=timeframe,
                            auto_adjust=False,
                            progress=False
                        )
                    else:
                        df = yf.download(
                            download_symbol,
                            start=start_date,
                            end=end_date,
                            interval=timeframe,
                            auto_adjust=False,
                            progress=False
                        )

                    if df is None or df.empty:
                        log(f"No data downloaded: {download_symbol} | {timeframe}")
                        failed_symbols.append(download_symbol)
                        continue

                    # Fix MultiIndex columns if present
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)

                    df.reset_index(inplace=True)
                    df.to_csv(csv_path, index=False)

                except Exception as e:
                    log(f"Download failed: {download_symbol} | {timeframe} | {e}")
                    traceback.print_exc()
                    failed_symbols.append(download_symbol)

        # -------------------------------
        # LOG ALL FAILED SYMBOLS AT END
        # -------------------------------
        if failed_symbols:
            failed_symbols_str = ",".join(failed_symbols)
            log(f"❌ DOWNLOAD FAILED SYMBOLS: {failed_symbols_str}")
            print(f"\n❌ Download failed for {len(failed_symbols)} symbols. See log for details.")

        print(f"\n🎉 Download complete for {asset_type.upper()} | mode={mode.upper()}")
        log(f"Download complete for {asset_type.upper()}")

    except Exception as e:
        log(f"Download process failed: {e}")
        traceback.print_exc()

    finally:
        if conn:
            close_db_connection(conn)