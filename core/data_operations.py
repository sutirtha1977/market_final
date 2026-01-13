from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt


from config.logger import log, clear_log
from config.nse_constants import DATA_MENU_ITEMS
# from config.nse_constants import NSE_INDICES, US_INDICES, US_COMMODITIES
from db.create_db import create_stock_database
from services.symbol_service import refresh_symbols
from services.equity_service import insert_equity_price_data_pipeline
from services.index_service import insert_index_price_data_pipeline
from services.asset_service import insert_asset_price_data_pipeline
# from services.commodity_service import insert_commodity_price_data_pipeline
from services.indicator_service import refresh_indicators
from services.weekly_monthly_service import refresh_all_week52_stats
from services.bhavcopy_loader import (
    update_hist_delv_pct_from_bhavcopy,
    update_latest_delv_pct_from_bhavcopy
)
# from services.incremental_service import incr_yahoo_bhavcopy_download


console = Console()

# =====================================================================
# MENU DISPLAY  (SIMPLE STRUCTURE)
# =====================================================================
def display_menu() -> None:
    table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")
    # table.add_column("Frequency", justify="center")

    # for opt, action, freq in DATA_MENU_ITEMS:
    #     table.add_row(opt, action, freq)
    for opt, action in DATA_MENU_ITEMS:
        table.add_row(opt, action)
        
    panel = Panel(
        table,
        title="[bold blue]DATA OPERATIONS[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")

# Menu 1
def action_create_db() -> None:
    clear_log()
    console.print("[bold green]Database Creation Start...[/bold green]")
    create_stock_database()
    console.print("[bold green]Database Creation Finish...[/bold green]")
# Menu 2
def action_update_all_symbols() -> None:
    clear_log()
    console.print("[bold green]India Equity Symbols Insert Start...[/bold green]")
    refresh_symbols()
    console.print("[bold green]India Equity Symbols Insert Finish...[/bold green]")
# Menu 3
def action_update_india_equity_price() -> None:
    clear_log()
    syms = Prompt.ask("Enter symbols (ALL or comma-separated, e.g., RELIANCE,TCS)").upper()
    console.print("[bold green]India Equity Price Data Update Start....[/bold green]")
    insert_equity_price_data_pipeline(syms,asset_type="india_equity",mode="full")
    console.print("[bold green]India Equity Price Data Update Finish....[/bold green]")
# Menu 4
def action_update_usa_equity_price() -> None:
    clear_log()
    syms = Prompt.ask("Enter symbols (ALL or comma-separated, e.g., AMZN, AAPL)").upper()
    console.print("[bold green]USA Equity Price Data Update Start....[/bold green]")
    insert_equity_price_data_pipeline(syms,asset_type="usa_equity",mode="full")
    console.print("[bold green]USA Equity Price Data Update Finish....[/bold green]")
# Menu 5
def action_update_india_index_price() -> None:
    clear_log()
    console.print("[bold green]India Index Price Data Update Start....[/bold green]")
    insert_index_price_data_pipeline(asset_type="india_index",mode="full")
    console.print("[bold green]India Index Price Data Update Finish....[/bold green]")
# Menu 6
def action_update_global_index_price() -> None:
    clear_log()
    console.print("[bold green]USA Index Price Data Update Start....[/bold green]")
    insert_index_price_data_pipeline(asset_type="global_index",mode="full")
    console.print("[bold green]USA Index Price Data Update Finish....[/bold green]")
# Menu 7
def action_update_commodity_price() -> None:
    clear_log()
    console.print("[bold green]COMMODITY Price Data Update Start....[/bold green]") 
    insert_asset_price_data_pipeline(asset_type="commodity",mode="full")
    console.print("[bold green]COMMODITY Price Data Update Finish....[/bold green]")
# Menu 8
def action_update_crypto_price() -> None:
    clear_log()
    console.print("[bold green]CRYPTO Price Data Update Start....[/bold green]") 
    insert_asset_price_data_pipeline(asset_type="crypto",mode="full")
    console.print("[bold green]CRYPTO Price Data Update Finish....[/bold green]")
# Menu 9
def action_update_forex_price() -> None:
    clear_log()
    console.print("[bold green]FOREX Price Data Update Start....[/bold green]") 
    insert_asset_price_data_pipeline(asset_type="forex",mode="full")
    console.print("[bold green]FORES Price Data Update Finish....[/bold green]")
# Menu 10
def action_update_all_indicators() -> None:
    clear_log()
    console.print("[bold green]Update all Indicators Start....[/bold green]")
    refresh_indicators() 
    console.print("[bold green]Update all Indicators Finish....[/bold green]")
# Menu 11
def action_update_52week_stats() -> None:
    clear_log()
    console.print("[bold green]Refresh India and USA 52 WEEKS Start....[/bold green]") 
    refresh_all_week52_stats()
    console.print("[bold green]Refresh India and USA 52 WEEKS Finish....[/bold green]") 
# # Menu 12
def action_delv_pct_hist() -> None:
    clear_log()
    console.print("[bold green]India Equity Delivery % till 29-Dec-2025 update Start...[/bold green]") 
    update_hist_delv_pct_from_bhavcopy()
    console.print("[bold green]India Equity Delivery % till 29-Dec-2025 update Finish...[/bold green]") 
# Menu 13
def action_delv_pct_latest() -> None:
    clear_log()
    console.print("[bold green]India Equity Delivery % till 29-Dec-2025 update Start...[/bold green]") 
    update_latest_delv_pct_from_bhavcopy()
    console.print("[bold green]India Equity Delivery % till 29-Dec-2025 update Finish...[/bold green]") 
# =====================================================================
# MAIN LOOP
# =====================================================================
def data_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                break

            actions = {
                "1": action_create_db,
                "2": action_update_all_symbols,
                "3": action_update_india_equity_price,
                "4": action_update_usa_equity_price,
                "5": action_update_india_index_price,
                "6": action_update_global_index_price,
                "7": action_update_commodity_price,
                "8": action_update_crypto_price,
                "9": action_update_forex_price,
                "10": action_update_all_indicators,
                "11": action_update_52week_stats,
                "12": action_delv_pct_hist,
                "13": action_delv_pct_latest,
            }

            func = actions.get(choice)
            if func:
                func()
            else:
                console.print("[bold red]❌ Invalid choice![/bold red]")

    except KeyboardInterrupt:
        console.print("\n[bold green]Interrupted by user. Exiting...[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")

# =====================================================================
# ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    data_manager_user_input()