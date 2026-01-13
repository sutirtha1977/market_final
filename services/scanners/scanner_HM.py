import traceback
import pandas as pd
import os
from datetime import datetime, timedelta
from services.cleanup_service import delete_files_in_folder
from services.import_export_service import export_to_csv
from services.scanners.data_service import get_base_data
from config.paths import SCANNER_FOLDER_HM
from config.logger import log

LOOKBACK_DAYS = 60

#################################################################################################
# Applies the Hilega-Milega scanner rules to base data.
#################################################################################################
def apply_hilega_milega_logic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # Filter as per original logic
    df_filtered = df[
        (df['adj_close'] >= 100) &
        (df['adj_close'] < df['sma_20']) &
        (df['rsi_3'] / df['rsi_9'] >= 1.15) &
        (df['rsi_9'] / df['ema_rsi_9_3'] >= 1.04) &
        (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] >= 1) &
        (df['rsi_3'] < 60) &
        (df['rsi_3_weekly'] > 50) &
        (df['rsi_3_monthly'] > 50) &
        (df['pct_price_change'] <= 5)
    ].sort_values(['date','yahoo_symbol'], ascending=[False, True])

    return df_filtered

#################################################################################################
# Runs the Hilega-Milega scanner for all symbols using get_base_data.
# Exports the results to SCANNER_FOLDER.
#################################################################################################
def run_scanner_hilega_milega(
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

        # Format dates as strings
        start_date_str = start_date_dt.strftime("%Y-%m-%d")
        end_date_str = end_date_dt.strftime("%Y-%m-%d")

        # -------------------- CLEAN SCANNER FOLDER --------------------
        log("🧹 Clearing scanner folder...")
        folder_path = os.path.join(SCANNER_FOLDER_HM, asset_type)
        delete_files_in_folder(folder_path)

        # -------------------- FETCH BASE DATA --------------------
        log(f"🔍 Fetching base data for {start_date_str} to {end_date_str}...")
        df_base = get_base_data(start_date_str, end_date_str, asset_type)

        if df_base is None or df_base.empty:
            log(f"❌ No base data found for {start_date_str} to {end_date_str}")
            return pd.DataFrame()

        # Ensure required columns exist
        required_cols = [
            'adj_close', 'rsi_3', 'rsi_9', 'ema_rsi_9_3',
            'wma_rsi_9_21', 'rsi_3_weekly', 'rsi_3_monthly',
            'sma_20', 'pct_price_change'
        ]
        missing_cols = [c for c in required_cols if c not in df_base.columns]
        if missing_cols:
            log(f"❌ Missing required columns in base data: {missing_cols}")
            return pd.DataFrame()

        # -------------------- APPLY SCANNER LOGIC --------------------
        log("⚙️ Applying Hilega-Milega scanner logic...")
        df_signals = apply_hilega_milega_logic(df_base)

        if df_signals.empty:
            log(f"⚠ No stocks met scanner criteria for {start_date_str} to {end_date_str}")
            return pd.DataFrame()

        # -------------------- EXPORT RESULTS --------------------
        path = export_to_csv(df_signals, folder_path, "HM")
        log(f"✅ Hilega-Milega scanner results saved to: {path}")

        return df_signals

    except Exception as e:
        log(f"❌ scanner_hilega_milega failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()