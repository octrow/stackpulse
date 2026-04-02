from __future__ import annotations

from collections.abc import Iterable

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def make_table(title: str | None = None, *, expand: bool = False) -> Table:
    return Table(
        title=title,
        box=box.ROUNDED,
        header_style="bold cyan",
        border_style="bright_black",
        expand=expand,
        show_lines=False,
    )


def print_section(title: str) -> None:
    console.rule(f"[bold cyan]{title}[/bold cyan]")


def print_info(message: str) -> None:
    console.print(f"[cyan]{message}[/cyan]")


def print_success(message: str) -> None:
    console.print(f"[green]{message}[/green]")


def print_warning(message: str) -> None:
    console.print(f"[yellow]{message}[/yellow]")


def print_error(message: str) -> None:
    console.print(f"[red]{message}[/red]")


def print_panel(title: str, lines: Iterable[str], *, style: str = "cyan") -> None:
    body = "\n".join(lines)
    console.print(Panel.fit(body, title=title, border_style=style, padding=(1, 2)))


def percent_bar(percent: float, width: int = 16) -> str:
    clamped = max(0.0, min(100.0, percent))
    filled = int((clamped / 100.0) * width)
    return "█" * filled + "░" * (width - filled)
