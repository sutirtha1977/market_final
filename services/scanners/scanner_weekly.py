import traceback
from datetime import datetime, timedelta
import pandas as pd
import os
from services.cleanup_service import delete_files_in_folder
from services.import_export_service import export_to_csv
from services.scanners.data_service import get_base_data_weekly
from config.paths import SCANNER_FOLDER_WEEKLY
from config.logger import log

LOOKBACK_DAYS = 365

#################################################################################################
# APPLY SCANNER LOGIC
#################################################################################################

def apply_scanner_logic(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Filter as per original logic
        df_filtered = df[
            (df['close'] >= 100) &
            (df['rsi_3'] / df['rsi_9'] >= 1.15) &
            (df['rsi_9'] / df['ema_rsi_9_3'] >= 1.04) &
            (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] >= 1) &
            (df['rsi_3'] > 50)
        ].sort_values(['date','yahoo_symbol'], ascending=[False, True])

        return df_filtered  # Return filtered signals

    except Exception as e:
        log(f"❌ Error fetching weekly scanner data | {e}")
        traceback.print_exc()
        return pd.DataFrame()


#################################################################################################
# Runs the weekly momentum scanner for a given date range, exports results to CSV, 
# and returns the signals as a DataFrame.
#################################################################################################
def run_scanner_weekly(
    start_date: str | None = None,
    asset_type: str = "india_equity"
) -> pd.DataFrame:
    try:
        # -------------------- CALCULATE DATES --------------------
        today = datetime.today()
        end_date_dt = today
        start_date_dt = today - timedelta(days=LOOKBACK_DAYS)

        # Override with passed start_date if provided
        if start_date:
            start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")

        # Format dates as strings for SQL
        start_date_str = start_date_dt.strftime("%Y-%m-%d")
        end_date_str = end_date_dt.strftime("%Y-%m-%d")

        # -------------------- CLEAN SCANNER FOLDER --------------------
        log("🧹 Clearing scanner folder...")
        folder_path = os.path.join(SCANNER_FOLDER_WEEKLY, asset_type)
        delete_files_in_folder(folder_path)
        
        df_base = get_base_data_weekly(
            asset_type=asset_type,
            start_date=start_date_str, 
            end_date=end_date_str
        )

        # -------------------- RUN SCANNER --------------------
        log(f"🔍 Running weekly scanner from {start_date_str} to {end_date_str}")
        df_signals = apply_scanner_logic(df_base)

        if df_signals.empty:
            log(f"⚠ No weekly momentum signals found for {start_date_str} to {end_date_str}")
            return pd.DataFrame()

        # -------------------- EXPORT RESULTS --------------------
        path = export_to_csv(df_signals, folder_path, "WEEKLY")
        log(f"✅ Hilega-Milega scanner results saved to: {path}")

        return df_signals

    except Exception as e:
        log(f"❌ run_scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()