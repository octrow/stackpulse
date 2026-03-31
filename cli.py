from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.traceback import install as install_rich_traceback

install_rich_traceback(show_locals=False)

app = typer.Typer(help="StackPulse CLI")
console = Console()


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _venv_python(venv_path: Path) -> Path:
    if sys.platform.startswith("win"):
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


def _run_command(command: list[str], label: str, cwd: Path | None = None) -> None:
    cwd = cwd or _repo_root()
    with console.status(f"[bold cyan]{label}...", spinner="dots"):
        subprocess.run(command, cwd=cwd, check=True)


def _deps_installed(venv_python: Path) -> bool:
    probe = "import linkedin_scraper, playwright, dotenv, pandas, openpyxl, openai, typer, rich"
    try:
        subprocess.run(
            [str(venv_python), "-c", probe],
            cwd=_repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def _chromium_installed() -> bool:
    cache_dir = Path.home() / ".cache" / "ms-playwright"
    if not cache_dir.exists():
        return False
    return any(path.name.startswith("chromium-") for path in cache_dir.iterdir())


def _show_auto_summary(rows: list[tuple[str, str, str]]) -> None:
    table = Table(title="Auto workflow summary")
    table.add_column("Step", style="bold")
    table.add_column("Status")
    table.add_column("Details")
    for step, status, details in rows:
        table.add_row(step, status, details)
    console.print(table)


@app.command("setup-session")
def setup_session_command() -> None:
    """Create or refresh LinkedIn session.json."""
    try:
        from setup_session import main as setup_session_main

        with console.status(
            "[bold cyan]Running LinkedIn session setup...", spinner="dots"
        ):
            asyncio.run(setup_session_main())
        console.print("[green]Session setup complete.[/green]")
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]setup-session failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def scrape(
    limit: Optional[int] = typer.Option(
        None, "--limit", min=1, help="Max jobs per query"
    ),
    fresh: bool = typer.Option(False, "--fresh", help="Ignore previously scraped URLs"),
) -> None:
    """Run scraper using current config defaults unless overridden."""
    try:
        from config import JOBS_PER_QUERY
        from scrape import scrape_all

        effective_limit = limit if limit is not None else JOBS_PER_QUERY
        with console.status("[bold cyan]Running scrape workflow...", spinner="dots"):
            asyncio.run(scrape_all(limit_per_query=effective_limit, fresh=fresh))
        console.print("[green]Scrape completed.[/green]")
    except KeyboardInterrupt:
        console.print("[yellow]Scrape interrupted by user.[/yellow]")
        raise typer.Exit(code=130)
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]scrape failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def analyze(
    file: Optional[Path] = typer.Option(None, "--file", help="Specific jobs JSON file"),
    all_files: bool = typer.Option(
        False, "--all", help="Analyze all data/jobs_*.json files"
    ),
    llm: bool = typer.Option(
        False, "--llm", help="Enable open-taxonomy LLM extraction"
    ),
    promote: Optional[int] = typer.Option(
        None,
        "--promote",
        min=1,
        help="Promote pending LLM candidates with jobs_count >= N",
    ),
    candidates: bool = typer.Option(
        False,
        "--candidates",
        help="Show taxonomy candidates queue and exit",
    ),
) -> None:
    """Analyze scraped jobs and export Excel output."""
    if file and all_files:
        console.print("[red]Use either --file or --all, not both.[/red]")
        raise typer.Exit(code=1)

    try:
        import analyze as analyzer

        data_dir = Path(analyzer.OUTPUT_DIR)
        conn = analyzer.open_db(data_dir)
        analyzer.init_db(conn)

        if candidates:
            analyzer.print_candidates(conn)
            conn.close()
            return

        promote_only = promote is not None and not file and not all_files
        if promote_only:
            promote_threshold = promote if promote is not None else 1
            analyzer.apply_candidates(conn, promote_threshold)
            conn.close()
            return

        args = argparse.Namespace(file=str(file) if file else None, all=all_files)
        paths = analyzer._resolve_input_paths(args, data_dir)
        if paths is None:
            if promote is not None:
                analyzer.apply_candidates(conn, promote)
            conn.close()
            return

        if promote is not None:
            analyzer.apply_candidates(conn, promote)

        taxonomy = analyzer.load_taxonomy(conn)
        term_count = sum(len(terms) for terms in taxonomy.values())
        console.print(
            f"Taxonomy loaded: {term_count} terms (+ aliases) across {len(taxonomy)} categories"
        )

        console.print(f"Loading from: {[str(p) for p in paths]}")
        jobs = analyzer.load_jobs(paths)
        console.print(f"Loaded {len(jobs)} unique jobs.")

        if not jobs:
            conn.close()
            return

        llm_client = None
        if llm:
            llm_client = analyzer._build_llm_client(
                analyzer.NINEROUTER_BASE_URL,
                analyzer.NINEROUTER_MODEL,
            )

        df = analyzer.analyze(jobs, taxonomy, llm_client=llm_client, conn=conn)

        if llm and llm_client:
            analyzer.promote_llm_to_candidates(conn, threshold=2)

        conn.close()
        analyzer.print_report(df, taxonomy)

        output_stem = paths[0].stem if len(paths) == 1 else "jobs_all"
        analyzer.save_excel(df, data_dir / f"{output_stem}_analysis.xlsx", taxonomy)
        console.print("[green]Analysis completed.[/green]")
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]analyze failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def auto(
    venv: Path = typer.Option(Path(".venv"), "--venv", help="Virtual environment path"),
    python: str = typer.Option(
        sys.executable, "--python", help="Python executable for venv creation"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", min=1, help="Max jobs per query"
    ),
    fresh: bool = typer.Option(False, "--fresh", help="Ignore previously scraped URLs"),
    all_files: bool = typer.Option(False, "--all", help="Analyze all jobs files"),
    llm: bool = typer.Option(
        False, "--llm", help="Enable LLM extraction during analyze"
    ),
    promote: Optional[int] = typer.Option(
        None, "--promote", min=1, help="Promote pending candidates >= N"
    ),
) -> None:
    """Run bootstrap + session + scrape + analyze end-to-end."""
    rows: list[tuple[str, str, str]] = []
    repo_root = _repo_root()
    venv_path = (repo_root / venv).resolve() if not venv.is_absolute() else venv
    venv_python = _venv_python(venv_path)

    try:
        if not venv_python.exists():
            _run_command(
                [python, "-m", "venv", str(venv_path)],
                "Creating virtual environment",
                cwd=repo_root,
            )
            rows.append(("Create venv", "[green]done[/green]", str(venv_path)))
        else:
            rows.append(
                ("Create venv", "[yellow]skip[/yellow]", f"exists: {venv_path}")
            )

        if _deps_installed(venv_python):
            rows.append(
                (
                    "Install dependencies",
                    "[yellow]skip[/yellow]",
                    "requirements already satisfied",
                )
            )
        else:
            _run_command(
                [str(venv_python), "-m", "pip", "install", "-r", "requirements.txt"],
                "Installing dependencies",
                cwd=repo_root,
            )
            rows.append(
                (
                    "Install dependencies",
                    "[green]done[/green]",
                    "pip install -r requirements.txt",
                )
            )

        if _chromium_installed():
            rows.append(
                (
                    "Install Chromium",
                    "[yellow]skip[/yellow]",
                    "Playwright Chromium already present",
                )
            )
        else:
            _run_command(
                [str(venv_python), "-m", "playwright", "install", "chromium"],
                "Installing Playwright Chromium",
                cwd=repo_root,
            )
            rows.append(
                (
                    "Install Chromium",
                    "[green]done[/green]",
                    "playwright install chromium",
                )
            )

        from config import JOBS_PER_QUERY, SESSION_FILE

        session_path = repo_root / SESSION_FILE
        if session_path.exists():
            rows.append(
                ("Setup session", "[yellow]skip[/yellow]", f"exists: {SESSION_FILE}")
            )
        else:
            console.print(
                "[cyan]Session file missing. Starting interactive setup-session...[/cyan]"
            )
            subprocess.run(
                [str(venv_python), "setup_session.py"], cwd=repo_root, check=True
            )
            rows.append(("Setup session", "[green]done[/green]", SESSION_FILE))

        scrape_limit = limit if limit is not None else JOBS_PER_QUERY
        scrape_cmd = [str(venv_python), "scrape.py", "--limit", str(scrape_limit)]
        if fresh:
            scrape_cmd.append("--fresh")
        _run_command(scrape_cmd, "Running scrape", cwd=repo_root)
        rows.append(
            ("Scrape", "[green]done[/green]", f"limit={scrape_limit}, fresh={fresh}")
        )

        analyze_cmd = [str(venv_python), "analyze.py"]
        if all_files:
            analyze_cmd.append("--all")
        if llm:
            analyze_cmd.append("--llm")
        if promote is not None:
            analyze_cmd.extend(["--promote", str(promote)])

        _run_command(analyze_cmd, "Running analyze", cwd=repo_root)
        rows.append(
            (
                "Analyze",
                "[green]done[/green]",
                f"all={all_files}, llm={llm}, promote={promote if promote is not None else 'off'}",
            )
        )

        _show_auto_summary(rows)
        console.print("[bold green]Auto workflow completed.[/bold green]")
    except subprocess.CalledProcessError as exc:
        rows.append(
            (
                "Failure",
                "[red]failed[/red]",
                f"Exit code {exc.returncode}: {' '.join(exc.cmd)}",
            )
        )
        _show_auto_summary(rows)
        raise typer.Exit(code=exc.returncode) from exc
    except Exception as exc:  # noqa: BLE001
        rows.append(("Failure", "[red]failed[/red]", str(exc)))
        _show_auto_summary(rows)
        raise typer.Exit(code=1) from exc


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] == "analyze":
        typo_dash = any(arg == "-llm" for arg in argv[1:])
        typo_positional = len(argv) >= 2 and argv[1] == "llm"
        if typo_dash or typo_positional:
            console.print(
                "[red]Invalid LLM flag usage.[/red] Use: [bold]stackpulse analyze --llm[/bold]"
            )
            sys.exit(2)

    app()


if __name__ == "__main__":
    main()
