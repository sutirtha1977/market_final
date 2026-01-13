from datetime import datetime
import pandas as pd
import traceback
from config.logger import log
from db.connection import get_db_connection, close_db_connection
from config.db_table import SYMBOL_SOURCES,ASSET_TABLE_MAP

#################################################################################################
# Checks whether a given column exists in a PostgreSQL table using information_schema.
#################################################################################################  
def table_has_column(conn, table, column):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s
              AND column_name = %s
        """, (table, column))
        return cur.fetchone() is not None
#################################################################################################
# Loads symbols from a CSV and safely inserts/updates them into one symbol table, 
# reactivating entries if needed.
#################################################################################################
def refresh_one_symbol_table(table_name: str, csv_path):
    conn = None
    try:
        log(f"🔄 Refreshing {table_name} from {csv_path}")
        conn = get_db_connection()

        df = pd.read_csv(csv_path)
        df.columns = [c.lower().strip() for c in df.columns]

        required = {"name", "yahoo_symbol", "exchange"}
        if not required.issubset(df.columns):
            raise ValueError(
                f"{csv_path} must have columns {required}, found {set(df.columns)}"
            )

        df = df[list(required)].dropna().drop_duplicates()

        records = [
            (
                r["name"].strip(),
                r["yahoo_symbol"].strip().upper(),
                r["exchange"].strip().upper()
            )
            for _, r in df.iterrows()
        ]

        if not records:
            log(f"⚠️ No records in {csv_path}")
            return

        with conn.cursor() as cur:

            # -----------------------------------------
            # INSERT new symbols
            # -----------------------------------------
            cur.executemany(f"""
                INSERT INTO {table_name} (name, yahoo_symbol, exchange)
                VALUES (%s, %s, %s)
                ON CONFLICT (yahoo_symbol) DO NOTHING
            """, records)

            # -----------------------------------------
            # UPDATE missing fields
            # -----------------------------------------
            cur.executemany(f"""
                UPDATE {table_name}
                SET
                    name = %s,
                    exchange = %s
                WHERE yahoo_symbol = %s
                  AND (
                        name IS NULL OR name = '' OR
                        exchange IS NULL OR exchange = ''
                      )
            """, [
                (name, exchange, yahoo_symbol)
                for name, yahoo_symbol, exchange in records
            ])

            # -----------------------------------------
            # Reactivate if column exists
            # -----------------------------------------
            if table_has_column(conn, table_name, "is_active"):
                cur.executemany(
                    f"UPDATE {table_name} SET is_active = TRUE WHERE yahoo_symbol = %s",
                    [(r[1],) for r in records]
                )

        conn.commit()
        log(f"✅ {table_name}: {len(records)} symbols refreshed")

    except Exception as e:
        log(f"❌ Error refreshing {table_name}: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            close_db_connection(conn)
#################################################################################################
# Orchestrates a full refresh of all symbol tables by iterating through configured CSV sources.
#################################################################################################
def refresh_symbols():
    log("🚀 Starting full symbol refresh")

    for table, csv_path in SYMBOL_SOURCES:
        try:
            refresh_one_symbol_table(table, csv_path)
        except Exception:
            log(f"⚠️ Skipped {table} due to error")

    log("🎯 Symbol refresh completed")
#################################################################################################
# Orchestrates a full refresh of all symbol tables by iterating through configured CSV sources.
# Retrieves symbol IDs and codes for any asset type.
# Parameters:
#     symbol      : str : "ALL" or comma-separated symbols
#     conn        : DB connection
#     asset_type  : str : one of 
#                   ["india_equity", "usa_equity", "india_index", "usa_index",
#                    "commodity", "crypto", "forex"]
# Returns:
#     pd.DataFrame with columns: symbol_id, name, yahoo_symbol (if present)
#################################################################################################
def retrieve_symbols(symbol: str, conn, asset_type: str) -> pd.DataFrame:
    try:
        if asset_type not in ASSET_TABLE_MAP:
            raise ValueError(f"Unsupported asset_type: {asset_type}")

        # --- Get symbol table from ASSET_TABLE_MAP ---
        table = ASSET_TABLE_MAP[asset_type][0]  # symbol table is the first element in tuple

        select_cols = "symbol_id, name, yahoo_symbol"

        # --- Normalize input ---
        if not symbol or not symbol.strip():
            log("No symbol provided")
            return pd.DataFrame()

        symbol_clean = symbol.strip().upper()

        # --- Fetch all symbols ---
        if symbol_clean == "ALL":
            query = f"SELECT {select_cols} FROM {table} ORDER BY yahoo_symbol"
            df = pd.read_sql(query, conn)
            log(f"Retrieved all symbols | Count: {len(df)}")
            return df

        # --- Parse comma-separated list ---
        symbols_list = [s.strip().upper() for s in symbol.split(",") if s.strip()]
        if not symbols_list:
            log("No valid symbols parsed")
            return pd.DataFrame()

        placeholders = ", ".join(["%s"] * len(symbols_list))  # PostgreSQL style
        query = f"""
            SELECT {select_cols}
            FROM {table}
            WHERE symbol IN ({placeholders})
              AND is_active = TRUE
            ORDER BY yahoo_symbol
        """
        df = pd.read_sql(query, conn, params=tuple(symbols_list))
        log(f"Retrieved symbols | Count: {len(df)} | Symbols: {symbols_list}")
        return df

    except Exception as e:
        log(f"❌ RETRIEVE SYMBOL FAILED: {e}")
        traceback.print_exc()
        return pd.DataFrame()
#################################################################################################
# Fetches the most recent available trading date for a given asset type and timeframe.
# Parameters:
#     asset_type : str : one of
#         ["india_equity", "usa_equity", "india_index", "usa_index",
#          "commodity", "crypto", "forex"]
#     timeframe  : str : "1d", "1wk", "1mo", etc.
# Returns:
#     date or None
#################################################################################################
def get_latest_trading_date(asset_type: str, timeframe: str = "1d"):
    # if asset_type not in ASSET_PRICE_MAP:
    #     raise ValueError(f"Unsupported asset_type: {asset_type}")

    # table_name = ASSET_PRICE_MAP[asset_type]
    # ------------------------------
    # Validate asset_type and get price table
    # ------------------------------
    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    table_name = ASSET_TABLE_MAP[asset_type][1]  # 0 = symbol table, 1 = price table

    conn = None
    try:
        conn = get_db_connection()

        # ------------------------------
        # Build SQL query
        # ------------------------------
        # Only equity and index tables have is_active, others may not
        use_is_active = asset_type in {"india_equity", "usa_equity", "india_index", "usa_index"}

        sql = f"SELECT MAX(date) AS latest_date FROM {table_name} WHERE timeframe = %s"
        params = [timeframe]

        if use_is_active:
            sql += " AND is_active = TRUE"

        df = pd.read_sql(sql, conn, params=params)
        latest = df.iloc[0]["latest_date"]

        if not latest:
            return None

        # ------------------------------
        # Convert PostgreSQL date to Python date
        # ------------------------------
        if isinstance(latest, str):
            return datetime.strptime(latest, "%Y-%m-%d").date()
        return latest

    except Exception as e:
        log(f"❗ Error fetching latest trading date from {table_name}: {e}")
        return None

    finally:
        if conn:
            close_db_connection(conn)
#################################################################################################
# Returns the latest trading date where delivery percentage (delv_pct)
# is available for all symbols for the specified timeframe.
#################################################################################################
def get_latest_equity_date_no_delv(asset_type: str, timeframe: str = "1d"):
    conn = get_db_connection()
    try:
        sql = f"""
        SELECT MAX(date) AS latest_valid_date
        FROM {asset_type}_price_data
        WHERE timeframe = %s
          AND delv_pct IS NOT NULL;
        """

        # Use params with %s placeholder
        df = pd.read_sql(sql, conn, params=[timeframe])
        latest = df.iloc[0]["latest_valid_date"]

        if not latest:
            return None

        # If latest is a string, convert to date (sometimes read_sql returns string)
        if isinstance(latest, str):
            return datetime.strptime(latest, "%Y-%m-%d").date()
        else:
            return latest  # Already a date object

    except Exception as e:
        log(f"❗ Error fetching latest date: {e}")
        return None

    finally:
        close_db_connection(conn)