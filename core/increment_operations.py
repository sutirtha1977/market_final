from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

from config.logger import log, clear_log
from config.nse_constants import INCREMENT_MENU_ITEMS
from services.utility_service import show_latest_dates
from services.equity_service import insert_equity_price_data_pipeline
from services.index_service import insert_index_price_data_pipeline
from services.asset_service import insert_asset_price_data_pipeline
from services.indicator_service import refresh_indicators
from services.weekly_monthly_service import refresh_all_week52_stats

console = Console()

# =====================================================================
# MENU DISPLAY  (SIMPLE STRUCTURE)
# =====================================================================
def display_menu() -> None:
    table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    for opt, action in INCREMENT_MENU_ITEMS:
        table.add_row(opt, action)

    panel = Panel(
        table,
        title="[bold blue]INCREMENT DATA MANAGER[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")

# =====================================================================
# DATAFRAME DISPLAY
# =====================================================================
def print_df_rich(df, max_rows: int = 20) -> None:
    table = Table(show_header=True, header_style="bold magenta")
    for col in df.columns:
        table.add_column(str(col))

    for _, row in df.head(max_rows).iterrows():
        table.add_row(*[str(val) for val in row.values])

    console.print(table)

    if len(df) > max_rows:
        console.print(f"... [bold]{len(df) - max_rows}[/] more rows not shown", style="dim")

# =====================================================================
# SCANNER HANDLER
# =====================================================================
# Menu 1
def action_show_latest_date() -> None:
    show_latest_dates() 
# Menu 2
def action_increment_india_equity() -> None:
    clear_log()
    syms = Prompt.ask("Enter symbols (ALL or comma-separated, e.g., RELIANCE,TCS)").upper()
    console.print("[bold green]India Equity Price Data Update Start....[/bold green]")
    insert_equity_price_data_pipeline(syms,type="india_equity",mode="incr")
    console.print("[bold green]India Equity Price Data Update Finish....[/bold green]")
# Menu 3
def action_increment_usa_equity() -> None:
    clear_log()
    syms = Prompt.ask("Enter symbols (ALL or comma-separated, e.g., RELIANCE,TCS)").upper()
    console.print("[bold green]USA Equity Price Data Update Start....[/bold green]")
    insert_equity_price_data_pipeline(syms,type="usa_equity",mode="incr")
    console.print("[bold green]USA Equity Price Data Update Finish....[/bold green]")
# Menu 4
def action_increment_india_index() -> None:
    clear_log()
    console.print("[bold green]India Index Price Data Update Start....[/bold green]")
    insert_index_price_data_pipeline(type="india_index",mode="incr")
    console.print("[bold green]India Index Price Data Update Finish....[/bold green]") 
# Menu 5
def action_increment_global_index() -> None:
    clear_log()
    console.print("[bold green]USA Index Price Data Update Start....[/bold green]")
    insert_index_price_data_pipeline(type="global_index",mode="incr")
    console.print("[bold green]USA Index Price Data Update Finish....[/bold green]")
# Menu 6
def action_increment_commodity() -> None:
    clear_log()
    console.print("[bold green]COMMODITY Price Data Update Start....[/bold green]") 
    insert_asset_price_data_pipeline(type="commodity",mode="incr")
    console.print("[bold green]COMMODITY Price Data Update Finish....[/bold green]")
# Menu 7
def action_increment_crypto() -> None:
    clear_log()
    console.print("[bold green]CRYPTO Price Data Update Start....[/bold green]") 
    insert_asset_price_data_pipeline(type="crypto",mode="incr")
    console.print("[bold green]CRYPTO Price Data Update Finish....[/bold green]")
# Menu 8
def action_increment_forex() -> None:
    clear_log()
    console.print("[bold green]FOREX Price Data Update Start....[/bold green]") 
    insert_asset_price_data_pipeline(type="forex",mode="incr")
    console.print("[bold green]FOREX Price Data Update Finish....[/bold green]")
# Menu 9
def action_increment_indicators() -> None:
    clear_log()
    console.print("[bold green]Refresh India and USA Indicators Start....[/bold green]")
    refresh_indicators() 
    console.print("[bold green]Refresh India and USA Indicators Finish....[/bold green]")
# Menu 10
def action_increment_52weeks() -> None:
    clear_log()
    console.print("[bold green]Refresh India and USA 52 WEEKS Start....[/bold green]") 
    refresh_all_week52_stats()
    console.print("[bold green]Refresh India and USA 52 WEEKS Finish....[/bold green]") 
# =====================================================================
# MAIN LOOP (SCANNERS ONLY)
# =====================================================================
def increment_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                break

            actions = {
                "1": action_show_latest_date,
                "2": action_increment_india_equity,
                "3": action_increment_usa_equity,
                "4": action_increment_india_index,
                "5": action_increment_global_index,
                "6": action_increment_commodity,
                "7": action_increment_crypto,
                "8": action_increment_forex,
                "9": action_increment_indicators,
                "10": action_increment_52weeks,
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
    increment_manager_user_input()