from pathlib import Path

# =========================================================
# BASE PATHS
# =========================================================
BASE_DIR = Path(__file__).parent.parent

# ---------------- Directories ----------------
DATA_DIR = BASE_DIR / "data"
# ---------------- Bhavcopy Directories ----------------
BHAVCOPY_DIR = DATA_DIR / "bhavcopy" / "daily"
BHAVCOPY_DIR_HIST = DATA_DIR / "bhavcopy" / "equity_bhav_29Dec2025"
BHAVCOPY_DIR_DB = DATA_DIR / "bhavcopy" / "equity_bhav_29Dec2025_onwards"
# ---------------- Yahoo Directories ----------------
YAHOO_DIR = DATA_DIR / "yahoo"
YAHOO_SYMBOLS = YAHOO_DIR / "symbols"
# ---------------- Misc Directories ----------------
ANALYSIS_FOLDER = DATA_DIR / "analysis"
# ---------------- Scanner Directories ----------------
SCANNER_FOLDER = DATA_DIR / "scanner_results" 
SCANNER_FOLDER_WEEKLY = SCANNER_FOLDER / "weekly"
SCANNER_FOLDER_HM = SCANNER_FOLDER / "HM"
SCANNER_FOLDER_PLAY = SCANNER_FOLDER / "play"

# ---------------- Database ----------------
# DB_FILE = BASE_DIR / "db" / "markets.db"

# ---------------- CSV ----------------
INDIA_EQUITY = YAHOO_SYMBOLS / "india_equity_yahoo_symbols.csv"
USA_EQUITY = YAHOO_SYMBOLS / "usa_equity_yahoo_symbols.csv"
INDIA_INDEX = YAHOO_SYMBOLS / "india_index_yahoo_symbols.csv"
USA_INDEX = YAHOO_SYMBOLS / "usa_index_yahoo_symbols.csv"
GLOBAL_INDEX = YAHOO_SYMBOLS / "global_index_yahoo_symbols.csv"
COMMODITY_SYMBOLS = YAHOO_SYMBOLS / "commodity_yahoo_symbols.csv"
CRYPTO_SYMBOLS = YAHOO_SYMBOLS / "crypto_yahoo_symbols.csv"
FOREX_SYMBOLS = YAHOO_SYMBOLS / "forex_yahoo_symbols.csv"

# ---------------- Logging ----------------
LOG_FILE = BASE_DIR / "audit_trail.log"

# =========================================================
# Helper Functions
# =========================================================
def ensure_folder(path: Path):
    """Ensure the folder exists."""
    path.mkdir(parents=True, exist_ok=True)

# =========================================================
# Ensure required folders exist
# =========================================================
for p in [
    DATA_DIR,
    BHAVCOPY_DIR,
    BHAVCOPY_DIR_HIST,
    BHAVCOPY_DIR_DB,
    SCANNER_FOLDER,
    ANALYSIS_FOLDER,
]:
    ensure_folder(p)