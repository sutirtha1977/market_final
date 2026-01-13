from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

from config.logger import log, clear_log
from config.nse_constants import SCANNER_MENU_ITEMS, ALLOWED_TYPES
from services.utility_service import show_latest_dates
from services.scanners.backtest_service import backtest_daily_scanners
from services.scanners.scanner_HM import run_scanner_hilega_milega
from services.scanners.scanner_weekly import run_scanner_weekly
from services.scanners.scanner_play import scanner_play_multi_years

console = Console()

# =====================================================================
# MENU DISPLAY  (SIMPLE STRUCTURE)
# =====================================================================
def display_menu() -> None:
    table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    for opt, action in SCANNER_MENU_ITEMS:
        table.add_row(opt, action)

    panel = Panel(
        table,
        title="[bold blue]SCANNER MANAGER[/bold blue]",
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
def action_scanner(scanner_type: str) -> None:
    clear_log()
    # Menu 1
    if scanner_type == "HM":
        console.print("[bold yellow]Running Hilega Milega Scanner...[/bold yellow]")
        user_date = Prompt.ask("Enter start date (YYYY-MM-DD) or press Enter", default="").strip()
        asset_type = Prompt.ask("Enter either india_equity, usa_equity, commodity, crypto, forex", default="").strip()
        
        if asset_type not in ALLOWED_TYPES:
            console.print(f"[bold red]❌ Invalid asset type: '{asset_type}'[/bold red]")
            console.print(f"Allowed values: {', '.join(ALLOWED_TYPES)}")
            raise ValueError(f"Invalid asset type: {asset_type}")
        
        df = run_scanner_hilega_milega(
                    start_date=user_date, 
                    asset_type=asset_type
                )
        print_df_rich(df)
    # Menu 2
    elif scanner_type == "WEEK":
        clear_log()
        console.print("[bold yellow]Running Weekly Scanner...[/bold yellow]")
        user_date = Prompt.ask("Enter start date (YYYY-MM-DD) or press Enter", default="").strip()
        asset_type = Prompt.ask("Enter either india_equity, usa_equity, commodity, crypto, forex", default="").strip()
        
        if asset_type not in ALLOWED_TYPES:
            console.print(f"[bold red]❌ Invalid asset type: '{asset_type}'[/bold red]")
            console.print(f"Allowed values: {', '.join(ALLOWED_TYPES)}")
            raise ValueError(f"Invalid asset type: {asset_type}")
        
        df = run_scanner_weekly(
            start_date=user_date,
            asset_type=asset_type
            )
        print_df_rich(df)
    # Menu 3
    elif scanner_type == "PLAY":
        clear_log()
        console.print("[bold yellow]Running Multi-Year Scanner...[/bold yellow]")
        user_year = Prompt.ask("Enter start year:", default="2026").strip()
        user_lookback = Prompt.ask("Enter lookback count:", default="15").strip()
        asset_type = Prompt.ask("Enter either india_equity, usa_equity, commodity, crypto, forex", default="").strip()
        
        try:
            # Validate asset type
            if asset_type not in ALLOWED_TYPES:
                raise ValueError(f"Invalid asset type: {asset_type}")

            # Validate lookback input
            lookback_count = int(user_lookback)

        except ValueError as e:
            console.print(f"[bold red]❌ {e}[/bold red]")
            if "asset type" in str(e):
                console.print(f"Allowed values: {', '.join(ALLOWED_TYPES)}")
            return
        
        scanner_play_multi_years(
            start_year=user_year, 
            lookback_years = lookback_count, 
            asset_type=asset_type)

# =====================================================================
# MAIN LOOP (SCANNERS ONLY)
# =====================================================================
def scanner_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                break

            actions = {
                "1": lambda: action_scanner("HM"),
                "2": lambda: action_scanner("WEEK"),
                "3": lambda: action_scanner("PLAY"),
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
    scanner_manager_user_input()