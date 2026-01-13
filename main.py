from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

from config.nse_constants import MAIN_MENU_ITEMS
from config.logger import clear_log
from core.data_operations import data_manager_user_input
from core.increment_operations import increment_manager_user_input
from core.scanner_operations import scanner_manager_user_input

console = Console()

# =====================================================================
# MENU DISPLAY  (SIMPLE STRUCTURE)
# =====================================================================
def display_menu() -> None:
    table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    # Build rows from constants
    for key, action in MAIN_MENU_ITEMS:
        table.add_row(key, action)

    panel = Panel(
        table,
        title="[bold blue]MAIN MENU[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)

    # Only ONE instruction line
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")
# =====================================================================
# MAIN LOOP
# =====================================================================
def main():
    while True:
        display_menu()

        # 👇 keep Prompt.ask minimal so it doesn’t print extra text
        choice = Prompt.ask("👉").strip()

        if choice == "1":
            data_manager_user_input()

        elif choice == "2":
            increment_manager_user_input()
            
        elif choice == "3":
            scanner_manager_user_input()
            
        elif choice == "0":
            console.print("[bold green]Exiting...[/bold green]")
            clear_log()
            break

        else:
            console.print("[bold red]❌ Invalid choice![/bold red]")

# -------------------------------------------------
if __name__ == "__main__":
    main()