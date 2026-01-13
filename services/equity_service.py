import yfinance as yf
import os
import traceback
import pandas as pd
from services.cleanup_service import (
    delete_invalid_timeframe_rows, 
    delete_files_in_folder
)
from config.paths import YAHOO_DIR,BHAVCOPY_DIR,BHAVCOPY_DIR_DB
from config.logger import log
from services.yahoo_service import download_yahoo_data_all_timeframes
from services.symbol_service import get_latest_trading_date
from services.import_export_service import import_csv_to_db
from services.bhavcopy_loader import (
    download_missing_bhavcopies, 
    update_equity_delv_pct_from_bhavcopy
)
from services.cleanup_service import copy_files
from config.nse_constants import FREQUENCIES

# #################################################################################################
# Runs the complete equity price pipeline—cleans folders, downloads 
# Yahoo data (full or incremental), imports to DB, fixes weekly/monthly records, 
# and for incremental India runs also processes bhavcopies to update delivery data.
# mode = "full" → full refresh
# mode = "incr" → incremental refresh
# #################################################################################################
def insert_equity_price_data_pipeline(
    symbol="ALL",
    asset_type="india_equity",
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
        # 2. DETERMINE MODE: FULL vs INCREMENTAL
        # ------------------------------------------------------------------
        if mode == "incr":
            log("===== FETCH DAILY LATEST DATE STARTED =====")
            print("===== FETCH DAILY LATEST DATE STARTED =====")
            latest_dt = get_latest_trading_date(asset_type=asset_type,timeframe="1d")
            log(f"DAILY LATEST DATE IS: {latest_dt} =====")
            print(f"DAILY LATEST DATE IS: {latest_dt} =====")

        # ------------------------------------------------------------------
        # 3. YAHOO DOWNLOAD
        # ------------------------------------------------------------------
        log("===== YAHOO DOWNLOAD STARTED =====")
        print("===== YAHOO DOWNLOAD STARTED =====")

        if mode == "full":
            download_yahoo_data_all_timeframes(
                asset_type = asset_type,
                symbols = symbol, 
                mode= mode
            )
        else:
            download_yahoo_data_all_timeframes(
                asset_type = asset_type,
                symbols = symbol, 
                mode= mode,
                latest_dt=latest_dt
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
        delete_invalid_timeframe_rows("1wk", data_type="price", asset_type=asset_type)
        delete_invalid_timeframe_rows("1mo", data_type="price", asset_type=asset_type)
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

        # ------------------------------------------------------------------
        # 7. EXTRA STEPS (ONLY FOR INCREMENTAL + INDIA)
        # ------------------------------------------------------------------
        # if mode == "incr" and type == "india":

        #     log("===== BHAVCOPY DOWNLOAD STARTED =====")
        #     print("===== BHAVCOPY DOWNLOAD STARTED =====")
        #     download_missing_bhavcopies(latest_dt, type="india")
        #     log("===== BHAVCOPY DOWNLOAD FINISHED =====")
        #     print("===== BHAVCOPY DOWNLOAD FINISHED =====")

        #     log("===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY STARTED =====")
        #     print("===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY STARTED =====")
        #     update_equity_delv_pct_from_bhavcopy(symbol="ALL", type="india")
        #     log("===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY FINISHED =====")
        #     print("===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY FINISHED =====")

        #     log("===== COPY BHAVCOPY FILES STARTED =====")
        #     print("===== COPY BHAVCOPY FILES STARTED =====")
        #     copy_files(BHAVCOPY_DIR, BHAVCOPY_DIR_DB)
        #     log("===== COPY BHAVCOPY FILES FINISHED =====")
        #     print("===== COPY BHAVCOPY FILES FINISHED =====")

        #     log("===== DELETE FILES FROM BHAVCOPY FOLDERS STARTED =====")
        #     print("===== DELETE FILES FROM BHAVCOPY FOLDERS STARTED =====")
        #     delete_files_in_folder(BHAVCOPY_DIR)
        #     log("===== DELETE FILES FROM BHAVCOPY FOLDERS FINISHED =====")
        #     print("===== DELETE FILES FROM BHAVCOPY FOLDERS FINISHED =====")

    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()

