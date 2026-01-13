"""
Generic SQL template for inserting/updating technical indicators in PostgreSQL.
Works for all asset types.
"""

SQL_INSERT = {
    "generic": """
        INSERT INTO {indicator_table} (
            {col_id}, timeframe, date,
            sma_20, sma_50, sma_200,
            rsi_3, rsi_9, rsi_14,
            bb_upper, bb_middle, bb_lower,
            atr_14, supertrend, supertrend_dir,
            ema_rsi_9_3, wma_rsi_9_21, pct_price_change,
            macd, macd_signal
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT({col_id}, timeframe, date)
        DO UPDATE SET
            sma_20          = EXCLUDED.sma_20,
            sma_50          = EXCLUDED.sma_50,
            sma_200         = EXCLUDED.sma_200,
            rsi_3           = EXCLUDED.rsi_3,
            rsi_9           = EXCLUDED.rsi_9,
            rsi_14          = EXCLUDED.rsi_14,
            bb_upper        = EXCLUDED.bb_upper,
            bb_middle       = EXCLUDED.bb_middle,
            bb_lower        = EXCLUDED.bb_lower,
            atr_14          = EXCLUDED.atr_14,
            supertrend      = EXCLUDED.supertrend,
            supertrend_dir  = EXCLUDED.supertrend_dir,
            ema_rsi_9_3     = EXCLUDED.ema_rsi_9_3,
            wma_rsi_9_21    = EXCLUDED.wma_rsi_9_21,
            pct_price_change = EXCLUDED.pct_price_change,
            macd            = EXCLUDED.macd,
            macd_signal     = EXCLUDED.macd_signal
    """
}

# Map all asset types to the generic template
for key in ["india_equity", "usa_equity", "india_index", "global_index",
            "commodity", "crypto", "forex"]:
    SQL_INSERT[key] = SQL_INSERT["generic"]