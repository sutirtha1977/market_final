import yfinance as yf
import os
import traceback
import pandas as pd
from services.import_export_service import import_csv_to_db
from config.paths import YAHOO_DIR
from config.logger import log
from services.cleanup_service import (
    delete_invalid_timeframe_rows, 
    delete_files_in_folder
)
from services.symbol_service import get_latest_trading_date

from services.yahoo_service import download_yahoo_data_all_timeframes
from config.nse_constants import FREQUENCIES

#################################################################################################
# Runs the complete index price pipeline—cleans folders, downloads 
# Yahoo data (full or incremental), imports to DB, fixes weekly/monthly records, 
# and cleans up files
# mode = "full" → full refresh
# mode = "incr" → incremental refresh
#################################################################################################  
def insert_index_price_data_pipeline(
    asset_type="india_index",
    mode="full"   # "full" or "incr"
):
    try:
        # ------------------------------------------------------------------
        # 1. CLEAN YAHOO FOLDERS (COMMON)
        # ------------------------------------------------------------------
        log("===== DELETE YAHOO FILES FROM FOLDERS STARTED =====")
        print("===== DELETE YAHOO FILES FROM FOLDERS STARTED =====")
        for timeframe in FREQUENCIES:
            delete_files_in_folder(os.path.join(YAHOO_DIR, timeframe))
        log("===== DELETE YAHOO FILES FROM FOLDERS FINISHED =====")
        print("===== DELETE YAHOO FILES FROM FOLDERS FINISHED =====")

        # ------------------------------------------------------------------
        # 2. FETCH LATEST DATE (ONLY FOR INCREMENTAL)
        # ------------------------------------------------------------------
        if mode == "incr":
            log("===== FETCH DAILY LATEST DATE STARTED =====")
            print("===== FETCH DAILY LATEST DATE STARTED =====")
            latest_dt = get_latest_trading_date(asset_type=asset_type, timeframe="1d")
            log(f"DAILY LATEST DATE IS: {latest_dt} =====")
            print(f"DAILY LATEST DATE IS: {latest_dt} =====")

        # ------------------------------------------------------------------
        # 3. YAHOO DOWNLOAD
        # ------------------------------------------------------------------
        log("===== YAHOO DOWNLOAD STARTED =====")
        print("===== YAHOO DOWNLOAD STARTED =====")

        if mode == "full":
            download_yahoo_data_all_timeframes(
                asset_type=asset_type,
                symbols="ALL",
                mode=mode
            )
        else:
            download_yahoo_data_all_timeframes(
                asset_type=asset_type,
                symbols="ALL",
                mode=mode,
                latest_dt = latest_dt
            )

        log("===== YAHOO DOWNLOAD FINISHED =====")
        print("===== YAHOO DOWNLOAD FINISHED =====")

        # ------------------------------------------------------------------
        # 4. CSV → DB IMPORT
        # ------------------------------------------------------------------
        log("===== CSV TO DATABASE IMPORT STARTED =====")
        print("===== CSV TO DATABASE IMPORT STARTED =====")
        import_csv_to_db(asset_type=asset_type)
        log("===== CSV TO DATABASE IMPORT FINISHED =====")
        print("===== CSV TO DATABASE IMPORT FINISHED =====")

        # ------------------------------------------------------------------
        # 5. CLEAN INVALID WEEKLY / MONTHLY
        # ------------------------------------------------------------------
        log("===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        print("===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        delete_invalid_timeframe_rows("1wk", data_type="price", asset_type=asset_type, is_index=True)
        delete_invalid_timeframe_rows("1mo", data_type="price", asset_type=asset_type, is_index=True)
        log("===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        print("===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")

        # ------------------------------------------------------------------
        # 6. CLEAN YAHOO FOLDERS AGAIN
        # ------------------------------------------------------------------
        # log("===== DELETE YAHOO FILES FROM FOLDERS STARTED =====")
        # print("===== DELETE YAHOO FILES FROM FOLDERS STARTED =====")
        # for timeframe in FREQUENCIES:
        #     delete_files_in_folder(os.path.join(YAHOO_DIR, timeframe))
        # log("===== DELETE YAHOO FILES FROM FOLDERS FINISHED =====")
        # print("===== DELETE YAHOO FILES FROM FOLDERS FINISHED =====")

    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()