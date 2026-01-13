import csv
import yfinance as yf
from config.logger import log

# =====================================================
# CONFIG
# =====================================================
OUTPUT_FILE = "valid_global_indices_yahoo_symbols.csv"

# Common global indices
CANDIDATE_SYMBOLS = [
    # USA
    ("S&P 500", "^GSPC"),
    ("Dow Jones Industrial", "^DJI"),
    ("NASDAQ 100", "^NDX"),
    ("Russell 2000", "^RUT"),
    
    # Europe
    ("FTSE 100", "^FTSE"),
    ("DAX", "^GDAXI"),
    ("CAC 40", "^FCHI"),
    ("Euro Stoxx 50", "^STOXX50E"),
    
    # Asia
    ("Nikkei 225", "^N225"),
    ("Hang Seng", "^HSI"),
    ("Shanghai Composite", "000001.SS"),
    ("KOSPI", "^KS11"),
    ("S&P/ASX 200", "^AXJO"),
    
    # India
    ("NIFTY 50", "^NSEI"),
    ("NIFTY Bank", "^NSEBANK"),
    ("NIFTY IT", "^CNXIT"),
    ("NIFTY FMCG", "^CNXFMCG"),
    
    # Other major global indices
    ("TSX Composite", "^GSPTSE"),
    ("Bovespa", "^BVSP"),
    ("IPC Mexico", "^MXX"),
    ("FTSE MIB", "^FTSEMIB"),
    ("IBEX 35", "^IBEX"),
]

# =====================================================
# TEST SYMBOL IN YFINANCE
# =====================================================
def has_data(symbol):
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="5d", interval="1d")
        return df is not None and not df.empty
    except Exception:
        return False

# =====================================================
# MAIN
# =====================================================
def export_valid_global_indices():
    log(f"🔍 Testing {len(CANDIDATE_SYMBOLS)} global index symbols")

    valid_rows = []

    for name, sym in CANDIDATE_SYMBOLS:
        if has_data(sym):
            valid_rows.append((name, sym))
            log(f"✅ VALID: {name} → {sym}")
        else:
            log(f"❌ NO DATA: {name} → {sym}")

    log(f"🎯 Total valid global indices: {len(valid_rows)}")

    # ---------------------------
    # WRITE CSV
    # ---------------------------
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "yahoo_symbol"])
        writer.writerows(valid_rows)

    log(f"💾 Exported: {OUTPUT_FILE}")


if __name__ == "__main__":
    export_valid_global_indices()