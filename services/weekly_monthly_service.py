from db.connection import get_db_connection, close_db_connection
from config.logger import log
from config.db_table import ASSET_TABLE_MAP

#################################################################################################
# Refresh 52-week high/low stats for any asset in ASSET_TABLE_MAP (PostgreSQL version)
#################################################################################################
def refresh_week52_high_low_stats(asset_key: str):
    """
    asset_key: key from ASSET_TABLE_MAP, e.g., 'india_equity', 'crypto', 'forex'
    """
    if asset_key not in ASSET_TABLE_MAP:
        log(f"❌ Unknown asset_key: {asset_key}")
        return

    symbol_table, price_table, _, stats_table = ASSET_TABLE_MAP[asset_key]
    col_id = "symbol_id"  # all tables use symbol_id

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        log(f"📊 Updating 52W stats for {price_table}")

        # -----------------------------
        # Get all symbols with daily data
        # -----------------------------
        cur.execute(f"""
            SELECT DISTINCT {col_id}
            FROM {price_table}
            WHERE timeframe = '1d'
        """)
        ids = [r[0] for r in cur.fetchall()]
        if not ids:
            log(f"⚠ No daily data found in {price_table}, skipping")
            return

        # -----------------------------
        # Fetch 52-week high/low
        # -----------------------------
        placeholders = ','.join(['%s'] * len(ids))
        cur.execute(f"""
            SELECT {col_id}, MAX(high), MIN(low)
            FROM {price_table}
            WHERE timeframe = '1d'
              AND {col_id} IN ({placeholders})
              AND date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY {col_id}
        """, ids)

        results = [(sid, high, low) for sid, high, low in cur.fetchall() if high is not None]
        if not results:
            log(f"⚠ No 52W data found in {price_table}")
            return

        # -----------------------------
        # UPSERT into stats table
        # -----------------------------
        for sid, high52, low52 in results:
            cur.execute(f"""
                INSERT INTO {stats_table} (
                    {col_id}, week52_high, week52_low, as_of_date
                )
                VALUES (%s, %s, %s, CURRENT_DATE)
                ON CONFLICT ({col_id}) DO UPDATE SET
                    week52_high = EXCLUDED.week52_high,
                    week52_low  = EXCLUDED.week52_low,
                    as_of_date  = EXCLUDED.as_of_date
            """, (sid, high52, low52))

        conn.commit()
        log(f"✅ {stats_table}: Updated {len(results)} rows")

    except Exception as e:
        if conn:
            conn.rollback()
        log(f"❌ 52W update failed for {asset_key}: {e}")
        import traceback; traceback.print_exc()

    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
        if conn:
            close_db_connection(conn)


#################################################################################################
# Refresh all 52-week stats for all assets
#################################################################################################
def refresh_all_week52_stats():
    for asset_key in ASSET_TABLE_MAP.keys():
        refresh_week52_high_low_stats(asset_key)