from __future__ import annotations

from collections.abc import Iterable

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()
_DISPLAY_MODE = "detailed"


def set_display_mode(mode: str) -> None:
    global _DISPLAY_MODE
    _DISPLAY_MODE = "compact" if mode == "compact" else "detailed"


def is_compact() -> bool:
    return _DISPLAY_MODE == "compact"


def make_table(title: str | None = None, *, expand: bool = False) -> Table:
    return Table(
        title=title,
        box=box.SIMPLE_HEAVY if is_compact() else box.ROUNDED,
        header_style="bold bright_cyan",
        border_style="bright_blue",
        title_style="bold bright_magenta",
        expand=expand,
        show_lines=False,
        pad_edge=False,
        collapse_padding=True,
    )


def print_section(title: str) -> None:
    icon = "◆" if not is_compact() else "▸"
    console.rule(f"[bold bright_cyan]{icon} {title}[/bold bright_cyan]")


def print_info(message: str) -> None:
    console.print(f"[bright_blue]▸ {message}[/bright_blue]")


def print_success(message: str) -> None:
    console.print(f"[green]✓ {message}[/green]")


def print_warning(message: str) -> None:
    console.print(f"[yellow]▲ {message}[/yellow]")


def print_error(message: str) -> None:
    console.print(f"[red]✕ {message}[/red]")


def print_panel(
    title: str, lines: Iterable[str], *, style: str = "bright_blue"
) -> None:
    body = "\n".join(lines)
    padding = (0, 1) if is_compact() else (1, 1)
    console.print(Panel.fit(body, title=title, border_style=style, padding=padding))


def percent_bar(percent: float, width: int = 16) -> str:
    bar_width = 10 if is_compact() else width
    clamped = max(0.0, min(100.0, percent))
    filled = int((clamped / 100.0) * bar_width)
    return "█" * filled + "░" * (bar_width - filled)


def metric_title(text: str) -> str:
    return f"◈ {text}" if not is_compact() else f"• {text}"


def help_hint(text: str) -> None:
    console.print(f"[dim]… {text}[/dim]")
