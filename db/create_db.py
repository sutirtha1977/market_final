from config.logger import log
from db.connection import get_db_connection, close_db_connection

# =====================================================================
# Creates or updates the multi-asset PostgreSQL database schema
# (ALL numeric columns as REAL instead of NUMERIC(12,2))
# =====================================================================
def create_stock_database(drop_existing=True):

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # =================================================
        # TABLE LISTS
        # =================================================
        symbol_tables = [
            "india_equity_symbols",
            "india_index_symbols",
            "usa_equity_symbols",
            "global_index_symbols",
            "commodity_symbols",
            "crypto_symbols",
            "forex_symbols"
        ]

        data_tables = [
            "india_equity_price_data", "india_equity_indicators", "india_equity_52week_stats",
            "india_index_price_data",  "india_index_indicators",  "india_index_52week_stats",
            "usa_equity_price_data",   "usa_equity_indicators",   "usa_equity_52week_stats",
            "global_index_price_data", "global_index_indicators", "global_index_52week_stats",
            "commodity_price_data",    "commodity_indicators",    "commodity_52week_stats",
            "crypto_price_data",       "crypto_indicators",       "crypto_52week_stats",
            "forex_price_data",        "forex_indicators",        "forex_52week_stats",
        ]

        # =================================================
        # DROP TABLES (if requested)
        # =================================================
        if drop_existing:
            log("⚠️ Dropping existing tables...")

            # Drop child tables first (FK dependency order)
            for table in data_tables:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

            for table in symbol_tables:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

            log("🗑 All existing tables dropped")

        # =================================================
        # CREATE SYMBOL TABLES
        # =================================================
        for table in symbol_tables:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol_id    SERIAL PRIMARY KEY,
                name         TEXT NOT NULL,
                yahoo_symbol TEXT UNIQUE NOT NULL,
                exchange     TEXT,
                is_active    BOOLEAN DEFAULT TRUE,
                is_future    BOOLEAN DEFAULT FALSE
            );
            """)
            log(f"🆕 Ensured symbols table: {table}")

        # =================================================
        # TABLE FACTORIES WITH REAL
        # =================================================
        def create_price_table(table_name, symbol_table):
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                symbol_id INTEGER,
                timeframe TEXT,
                date DATE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adj_close REAL,
                volume REAL,
                delv_pct REAL,
                is_future BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (symbol_id, timeframe, date),
                FOREIGN KEY(symbol_id) REFERENCES {symbol_table}(symbol_id)
            );
            """)

        def create_indicator_table(table_name, symbol_table):
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                symbol_id INTEGER,
                timeframe TEXT,
                date DATE,
                sma_20 REAL,
                sma_50 REAL,
                sma_200 REAL,
                rsi_3 REAL,
                rsi_9 REAL,
                rsi_14 REAL,
                macd REAL,
                macd_signal REAL,
                bb_upper REAL,
                bb_middle REAL,
                bb_lower REAL,
                atr_14 REAL,
                supertrend REAL,
                supertrend_dir INTEGER,
                ema_rsi_9_3 REAL,
                wma_rsi_9_21 REAL,
                pct_price_change REAL,
                PRIMARY KEY (symbol_id, timeframe, date),
                FOREIGN KEY(symbol_id) REFERENCES {symbol_table}(symbol_id)
            );
            """)

        def create_52week_table(table_name, symbol_table):
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                symbol_id INTEGER PRIMARY KEY,
                week52_high REAL,
                week52_low REAL,
                as_of_date DATE,
                FOREIGN KEY(symbol_id) REFERENCES {symbol_table}(symbol_id)
            );
            """)

        # =================================================
        # TABLE CONFIG
        # =================================================
        tables_config = [
            # India
            ("india_equity_symbols", "india_equity_price_data", "india_equity_indicators", "india_equity_52week_stats"),
            ("india_index_symbols",  "india_index_price_data",  "india_index_indicators",  "india_index_52week_stats"),

            # USA
            ("usa_equity_symbols", "usa_equity_price_data", "usa_equity_indicators", "usa_equity_52week_stats"),
            
            # GLOBAL INDEX
            ("global_index_symbols",  "global_index_price_data",  "global_index_indicators",  "global_index_52week_stats"),

            # Commodity
            ("commodity_symbols", "commodity_price_data", "commodity_indicators", "commodity_52week_stats"),

            # Crypto
            ("crypto_symbols", "crypto_price_data", "crypto_indicators", "crypto_52week_stats"),

            # Forex
            ("forex_symbols", "forex_price_data", "forex_indicators", "forex_52week_stats"),
        ]

        # =================================================
        # CREATE ALL DATA TABLES
        # =================================================
        for sym_table, price_table, ind_table, stats_table in tables_config:
            create_price_table(price_table, sym_table)
            create_indicator_table(ind_table, sym_table)
            create_52week_table(stats_table, sym_table)
            log(f"✅ Ensured tables for {sym_table}")

        conn.commit()
        log("🎉 PostgreSQL multi-asset database created/updated successfully with REAL numeric columns")

    except Exception as e:
        conn.rollback()
        log(f"❌ DB creation/update failed: {e}")
        raise

    finally:
        close_db_connection(conn)