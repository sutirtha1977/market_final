# NSE-specific constants

# ---------------- Menu and colors ----------------
FREQ_COLORS = {
    "Run Once": "bold blue",
    "Run Daily": "bold green",
    "Run As Required": "bold yellow",
    "": "white"
}

MAIN_MENU_ITEMS = [
    ("DATA MANAGER", "[bold]ENTER 1[/bold]"),
    ("INCREMENT DATA MANAGER", "[bold]ENTER 2[/bold]"),
    ("SCANNERS", "[bold]ENTER 3[/bold]"),
    ("EXIT PROGRAM", "[bold red]ENTER 0[/bold red]"),
]

DATA_MENU_ITEMS = [
    ("CREATE DATABASE FROM SCRATCH", "[bold]ENTER 1[/bold]"),
    ("UPDATE ALL SYMBOLS", "[bold]ENTER 2[/bold]"),
    ("UPDATE INDIA EQUITY PRICE DATA", "[bold]ENTER 3[/bold]"),
    ("UPDATE US EQUITY PRICE DATA", "[bold]ENTER 4[/bold]"),
    ("UPDATE INDIA INDEX PRICE DATA", "[bold]ENTER 5[/bold]"),
    ("UPDATE GLOBAL INDEX PRICE DATA", "[bold]ENTER 6[/bold]"),
    ("UPDATE COMMODITY PRICE DATA", "[bold]ENTER 7[/bold]"),
    ("UPDATE CRYPTO PRICE DATA", "[bold]ENTER 8[/bold]"),
    ("UPDATE FOREX PRICE DATA", "[bold]ENTER 9[/bold]"),    
    ("UPDATE ALL INDICATORS", "[bold]ENTER 10[/bold]"),
    ("UPDATE ALL 52 WEEK STATS", "[bold]ENTER 11[/bold]"),
    ("UPDATE INDIA EQUITY DELIVERY % TILL 29-DEC-2025", "[bold]ENTER 12[/bold]"),
    ("UPDATE INDIA EQUITY DELIVERY % TILL DATE", "[bold]ENTER 13[/bold]"),
    ("BACK TO MAIN MENU",  "[bold red]ENTER 0[/bold red]"),
]
INCREMENT_MENU_ITEMS = [
    ("SHOW LATEST DATES FOR TABLES", "[bold green]ENTER 1[/bold green]"),
    ("INCREMENTAL UPDATE OF INDIA EQUITY", "[bold green]ENTER 2[/bold green]"),
    ("INCREMENTAL UPDATE OF USA EQUITY", "[bold green]ENTER 3[/bold green]"),
    ("INCREMENTAL UPDATE OF INDIA INDEX", "[bold green]ENTER 4[/bold green]"),
    ("INCREMENTAL UPDATE OF GLOBAL INDEX", "[bold green]ENTER 5[/bold green]"),
    ("INCREMENTAL UPDATE OF COMMODITY", "[bold green]ENTER 6[/bold green]"),
    ("INCREMENTAL UPDATE OF CRYPTO", "[bold green]ENTER 7[/bold green]"),
    ("INCREMENTAL UPDATE OF FOREX", "[bold green]ENTER 8[/bold green]"),
    ("INCREMENTAL UPDATE OF INDICATORS", "[bold green]ENTER 9[/bold green]"),
    ("INCREMENTAL UPDATE OF 52 WEEKS STATS", "[bold green]ENTER 10[/bold green]"),
    ("BACK TO MAIN MENU",  "[bold red]ENTER 0[/bold red]"),
]
SCANNER_MENU_ITEMS = [
    ("HILEGA MILEGA SCANNER", "[bold green]ENTER 1[/bold green]"),
    ("WEEKLY SCANNER", "[bold green]ENTER 2[/bold green]"),
    ("SCANNER PLAYGROUND", "[bold green]ENTER 3[/bold green]"),
    ("BACK TO MAIN MENU",  "[bold red]ENTER 0[/bold red]"),
]
ALLOWED_TYPES = {"india_equity", "usa_equity", "commodity", "crypto", "forex"}
ALLOWED_INDEX_TYPES = {"india_index", "global_index"}
# ---------------- Frequencies ----------------
FREQUENCIES = ["1d", "1wk", "1mo"]

# ---------------- NSE URLs ----------------
NSE_URL_BHAV_DAILY = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"
SNP_500 = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
NASDAQ_100 = "https://en.wikipedia.org/wiki/Nasdaq-100"