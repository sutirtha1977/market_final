import traceback
from datetime import datetime
import pandas as pd
import os
from services.cleanup_service import delete_files_in_folder
from services.import_export_service import export_to_csv
from services.scanners.backtest_service import (
    backtest_weekly_scanners, 
    backtest_daily_scanners
)
from services.scanners.data_service import get_base_data_weekly
from config.paths import SCANNER_FOLDER_PLAY
from config.logger import log

LOOKBACK_DAYS = 365

#################################################################################################
# Applies the filters on data to identify qualifying stocks.
#################################################################################################
def apply_scanner_logic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # ---------------------------------------------------
    # CHANGE CODE
    # ---------------------------------------------------
    df_filtered = df[
        (df['close'] >= 100)
        & (df['rsi_3'] / df['rsi_9'] >= 1.15)
        & (df['rsi_9'] / df['ema_rsi_9_3'] >= 1.04)
        & (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] >= 1)
        & (df['rsi_3'] > 50)
    ].sort_values(['date','yahoo_symbol'], ascending=[False, True])
    # ---------------------------------------------------
    # CHANGE CODE
    # ---------------------------------------------------

    return df_filtered


#################################################################################################
# Fetches data, applies scanner rules, classifies candlestick patterns, 
# and exports qualifying signals to CSV.
#################################################################################################
def run_scanner(
    start_date: str | None = None, 
    end_date: str | None = None, 
    file_name: str | None = None,
    asset_type: str = "india_equity",
    folder_path: str | None = None, 
) -> pd.DataFrame:
    try:
        log("🔍 Fetching base data...")

        # ---------------------------------------------------
        # CHANGE CODE
        # ---------------------------------------------------
        df_base = get_base_data_weekly(
                    asset_type=asset_type,
                    start_date=start_date, 
                    end_date=end_date
                )

        # ---------------------------------------------------
        # CHANGE CODE
        # ---------------------------------------------------
        if df_base is None or df_base.empty:
            log(f"❌ No base data found for end date: {start_date}")
            return pd.DataFrame()

        log("⚙️ Applying Scanner logic...")
        df_signals = apply_scanner_logic(df_base)
        
        if df_signals.empty:
            log(f"⚠ No stocks met scanner criteria for end date: {start_date}")
            return pd.DataFrame()

        # ---------------------------------------------------
        # ADD CANDLE TYPE (CORE CHANGE)
        # ---------------------------------------------------
        # required_cols = {"open", "high", "low", "close"}
        # if not required_cols.issubset(df_signals.columns):
        #     log("❌ Missing OHLC columns for candle detection")
        #     return pd.DataFrame()

        # df_signals["candle_type"] = df_signals.apply(
        #     lambda r: get_candle_type(
        #         r["open"],
        #         r["high"],
        #         r["low"],
        #         r["close"]
        #     ),
        #     axis=1
        # )
        # ---------------------------------------------------

        path = export_to_csv(df_signals, str(folder_path), str(file_name))
        log(f"✅ Scanner results saved to: {path}")

        return df_signals

    except Exception as e:
        log(f"❌ Scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()

#################################################################################################
# Runs the scanner year-by-year across multiple years, aggregates results, 
# and performs backtesting on all generated signals.
#################################################################################################
def scanner_play_multi_years(
    start_year: str, 
    lookback_years: int,
    asset_type: str = "india_equity"
):
    try:
        log("🧹 Clearing scanner folder...")
        folder_path = os.path.join(SCANNER_FOLDER_PLAY, asset_type)
        delete_files_in_folder(folder_path)

        start_year_int = int(start_year)
        all_years_results = []

        for i in range(lookback_years):
            year = start_year_int - i
            start_date = f"{year}-01-01"
            end_date   = f"{year}-12-31"

            print(f"\n🔹 YEAR {year}")

            df_year = run_scanner(
                    start_date = start_date,
                    end_date = end_date, 
                    file_name=str(year), 
                    asset_type=asset_type,
                    folder_path = folder_path
                )
            
            print(f"➡ Rows found: {len(df_year)}")

            if not df_year.empty:
                df_year["year"] = year
                all_years_results.append(df_year)

        if all_years_results:
            final_df = pd.concat(all_years_results, ignore_index=True)
            print(f"✅ TOTAL rows across years: {len(final_df)}")
        else:
            final_df = pd.DataFrame()
            print("⚠ No results across years")
        # Weekly Scanner Backtest
        df_backtest = backtest_weekly_scanners(asset_type=asset_type,folder_path=folder_path)
        # Daily Scanner Backtest
        # df_backtest = backtest_daily_scanners(asset_type=asset_type,folder_path=folder_path)


        return final_df

    except Exception as e:
        print(f"❌ Multi-year scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()
