from rich.table import Table
from rich.console import Console
from db.connection import get_db_connection, close_db_connection
from config.db_table import DATA_TABLES

def show_latest_dates():
    console = Console()
    conn = get_db_connection()
    cur = conn.cursor()

    # # Table list for all asset types
    # tables = [
    #     "india_equity_price_data",
    #     "india_equity_indicators",
    #     "usa_equity_price_data",
    #     "usa_equity_indicators",
    #     "india_index_price_data",
    #     "india_index_indicators",
    #     "usa_index_price_data",
    #     "usa_index_indicators",
    #     "commodity_price_data",
    #     "commodity_indicators",
    #     "crypto_price_data",
    #     "crypto_indicators",
    #     "forex_price_data",
    #     "forex_indicators",
    # ]

    # Create Rich table
    table = Table(title="📊 Latest Data Availability", show_lines=True)
    table.add_column("Table", style="bold cyan")
    table.add_column("1D", justify="center")
    table.add_column("1WK", justify="center")
    table.add_column("1MO", justify="center")

    try:
        for tbl in DATA_TABLES:
            sql = f"""
                SELECT
                    MAX(CASE WHEN timeframe = '1d'  THEN date END) AS d1,
                    MAX(CASE WHEN timeframe = '1wk' THEN date END) AS d1w,
                    MAX(CASE WHEN timeframe = '1mo' THEN date END) AS d1m
                FROM {tbl}
            """
            cur.execute(sql)
            r = cur.fetchone()

            d1  = r[0].strftime("%Y-%m-%d") if r[0] else "-"
            d1w = r[1].strftime("%Y-%m-%d") if r[1] else "-"
            d1m = r[2].strftime("%Y-%m-%d") if r[2] else "-"

            table.add_row(tbl, d1, d1w, d1m)

        console.print(table)

    finally:
        cur.close()
        close_db_connection(conn)