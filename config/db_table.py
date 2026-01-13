from config.paths import (
    INDIA_EQUITY,
    USA_EQUITY,
    INDIA_INDEX,
    # USA_INDEX,
    GLOBAL_INDEX,
    COMMODITY_SYMBOLS,
    CRYPTO_SYMBOLS,
    FOREX_SYMBOLS
)
DB_CONFIG = {
    "host": "localhost",
    "dbname": "market_data",
    "user": "sutirtha",
    "password": "1977",
    "port": 5432
}
SYMBOL_SOURCES = [
    ("india_equity_symbols", INDIA_EQUITY),
    ("usa_equity_symbols",   USA_EQUITY),
    ("india_index_symbols",  INDIA_INDEX),
    ("global_index_symbols", GLOBAL_INDEX),
    ("commodity_symbols",    COMMODITY_SYMBOLS),
    ("crypto_symbols",       CRYPTO_SYMBOLS),
    ("forex_symbols",        FOREX_SYMBOLS),
]
# ASSET_SYMBOL_MAP = {
#     "india_equity": "india_equity_symbols",
#     "usa_equity": "usa_equity_symbols",
#     "india_index": "india_index_symbols",
#     "global_index": "global_index_symbols",
#     "commodity": "commodity_symbols",
#     "crypto": "crypto_symbols",
#     "forex": "forex_symbols"
# }
# ASSET_PRICE_MAP = {
#     "india_equity": "india_equity_price_data",
#     "usa_equity": "usa_equity_price_data",
#     "india_index": "india_index_price_data",
#     "global_index": "global_index_price_data",
#     "commodity": "commodity_price_data",
#     "crypto": "crypto_price_data",
#     "forex": "forex_price_data"
# }
ASSET_PRICE_SYMBOL_MAP = {
    "india_equity": ("india_equity_price_data", "india_equity_symbols"),
    "usa_equity":   ("usa_equity_price_data",   "usa_equity_symbols"),
    "india_index":  ("india_index_price_data",  "india_index_symbols"),
    "global_index": ("global_index_price_data", "global_index_symbols"),
    "commodity":    ("commodity_price_data",    "commodity_symbols"),
    "crypto":       ("crypto_price_data",       "crypto_symbols"),
    "forex":        ("forex_price_data",        "forex_symbols"),
}
ASSET_TABLE_MAP = {
    "india_equity": ("india_equity_symbols",    "india_equity_price_data",  "india_equity_indicators",  "india_equity_52week_stats"),
    "usa_equity":   ("usa_equity_symbols",      "usa_equity_price_data",    "usa_equity_indicators",    "usa_equity_52week_stats"),
    "india_index":  ("india_index_symbols",     "india_index_price_data",   "india_index_indicators",   "india_index_52week_stats"),
    "global_index": ("global_index_symbols",    "global_index_price_data",  "global_index_indicators",  "global_index_52week_stats"),
    "commodity":    ("commodity_symbols",       "commodity_price_data",     "commodity_indicators",     "commodity_52week_stats"),
    "crypto":       ("crypto_symbols",          "crypto_price_data",        "crypto_indicators",        "crypto_52week_stats"),
    "forex":        ("forex_symbols",           "forex_price_data",         "forex_indicators",         "forex_52week_stats"),
}
DATA_TABLES = [
    "india_equity_price_data",
    "india_equity_indicators",
    "usa_equity_price_data",
    "usa_equity_indicators",
    "india_index_price_data",
    "india_index_indicators",
    "global_index_price_data",
    "global_index_indicators",
    "commodity_price_data",
    "commodity_indicators",
    "crypto_price_data",
    "crypto_indicators",
    "forex_price_data",
    "forex_indicators",
]
