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

from config import LLM_CANDIDATE_THRESHOLD

install_rich_traceback(show_locals=False)

app = typer.Typer(help="StackPulse CLI", invoke_without_command=True)
console = Console()


@app.callback(invoke_without_command=True)
def _default(ctx: typer.Context) -> None:
    """Run interactive wizard when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        _interactive_wizard()


def _interactive_wizard() -> None:
    """Prompt the user for what to run when stackpulse is called with no args."""
    console.print("\n[bold cyan]StackPulse[/bold cyan] — what would you like to do?\n")
    console.print("  1  analyze        Analyze scraped jobs and export stats")
    console.print("  2  scrape         Scrape LinkedIn for new jobs")
    console.print("  3  auto           Bootstrap + scrape + analyze end-to-end")
    console.print("  4  setup-session  Create or refresh LinkedIn session")
    console.print("  5  quit\n")

    choice = typer.prompt("Choice [1-5]", default="1")

    if choice == "5" or choice.lower() == "quit":
        raise typer.Exit()

    if choice == "2":
        limit_str = typer.prompt(
            "Max jobs per query (leave empty for config default)", default=""
        )
        limit = int(limit_str) if limit_str.strip().isdigit() else None
        fresh = typer.confirm(
            "Ignore previously scraped URLs (--fresh)?", default=False
        )
        scrape(limit=limit, fresh=fresh)

    elif choice == "3":
        limit_str = typer.prompt(
            "Max jobs per query (leave empty for config default)", default=""
        )
        limit = int(limit_str) if limit_str.strip().isdigit() else None
        fresh = typer.confirm(
            "Ignore previously scraped URLs (--fresh)?", default=False
        )
        all_files = typer.confirm("Analyze all jobs files (--all)?", default=True)
        llm = typer.confirm("Enable LLM extraction (--llm)?", default=False)
        auto(
            limit=limit,
            fresh=fresh,
            all_files=all_files,
            llm=llm,
            promote=None,
            venv=Path(".venv"),
            python=sys.executable,
        )

    elif choice == "4":
        setup_session_command()

    else:
        # Default: analyze
        all_files = typer.confirm("Analyze all jobs files (--all)?", default=True)
        llm = typer.confirm("Enable LLM extraction (--llm)?", default=False)
        title_raw = typer.prompt(
            "Filter by title contains (leave empty to skip)", default=""
        )
        location_raw = typer.prompt(
            "Filter by location contains (leave empty to skip)", default=""
        )
        title_contains = title_raw.strip() or None
        location_contains = location_raw.strip() or None
        analyze(
            file=None,
            all_files=all_files,
            llm=llm,
            promote=None,
            candidates=False,
            title_contains=title_contains,
            location_contains=location_contains,
        )


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


def _run_async(coro):
    """Run an async coroutine, suppressing Playwright teardown noise on Ctrl+C."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _exc_handler(_loop, ctx):
        exc = ctx.get("exception")
        if exc is not None and "TargetClosedError" in type(exc).__name__:
            return
        _loop.default_exception_handler(ctx)

    loop.set_exception_handler(_exc_handler)

    orig_unraisablehook = sys.unraisablehook

    def _quiet_unraisable(item):
        if item.exc_type is RuntimeError and "Event loop is closed" in str(
            item.exc_value
        ):
            return
        orig_unraisablehook(item)

    sys.unraisablehook = _quiet_unraisable
    try:
        return loop.run_until_complete(coro)
    finally:
        try:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            loop.close()
            sys.unraisablehook = orig_unraisablehook


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
            _run_async(scrape_all(limit_per_query=effective_limit, fresh=fresh))
        console.print("[green]Scrape completed.[/green]")
    except KeyboardInterrupt as exc:
        console.print("[yellow]Scrape interrupted by user.[/yellow]")
        raise typer.Exit(code=130) from exc
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]scrape failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc


def _run_analysis_pipeline(
    analyzer,
    conn,
    paths: list[Path],
    data_dir: Path,
    promote: Optional[int],
    use_llm: bool,
    title_contains: Optional[str],
    location_contains: Optional[str],
) -> None:
    """Execute the core analysis pipeline: load → filter → analyze → report → export.

    Assumes DB is open; does NOT close it (caller's responsibility via try/finally).
    """
    if promote is not None:
        analyzer.apply_candidates(conn, promote)

    skills = analyzer.load_skills(conn)
    term_count = sum(len(terms) for terms in skills.values())
    console.print(
        f"Skills loaded: {term_count} terms (+ aliases) across {len(skills)} categories"
    )

    console.print(f"Loading from: {[str(p) for p in paths]}")
    jobs = analyzer.load_jobs(paths)
    console.print(f"Loaded {len(jobs)} unique jobs.")

    if title_contains:
        jobs = [
            j
            for j in jobs
            if title_contains.lower() in (j.get("job_title") or "").lower()
        ]
        console.print(f"After title filter '{title_contains}': {len(jobs)} jobs")
    if location_contains:
        jobs = [
            j
            for j in jobs
            if location_contains.lower() in (j.get("location") or "").lower()
        ]
        console.print(f"After location filter '{location_contains}': {len(jobs)} jobs")

    if not jobs:
        return

    llm_client = None
    if use_llm:
        llm_client = analyzer.build_llm_client(
            analyzer.NINEROUTER_BASE_URL,
            analyzer.NINEROUTER_MODEL,
        )

    df = analyzer.analyze(jobs, skills, llm_client=llm_client, conn=conn)

    if use_llm and llm_client:
        analyzer.promote_llm_to_candidates(conn, threshold=LLM_CANDIDATE_THRESHOLD)

    existing_candidate_terms = {
        row[0] for row in conn.execute("SELECT term FROM skill_candidates")
    }

    analyzer.print_report(df, skills, existing_candidate_terms, LLM_CANDIDATE_THRESHOLD)

    output_stem = paths[0].stem if len(paths) == 1 else "jobs_all"
    analyzer.save_excel(df, data_dir / f"{output_stem}_analysis.xlsx", skills)
    console.print("[green]Analysis completed.[/green]")


@app.command()
def analyze(
    file: Optional[Path] = typer.Option(None, "--file", help="Specific jobs JSON file"),
    all_files: bool = typer.Option(
        False, "--all", help="Analyze all data/jobs_*.json files"
    ),
    llm: bool = typer.Option(False, "--llm", help="Enable LLM skill extraction"),
    promote: Optional[int] = typer.Option(
        None,
        "--promote",
        min=1,
        help="Promote pending LLM candidates with jobs_count >= N",
    ),
    candidates: bool = typer.Option(
        False,
        "--candidates",
        help="Show skill candidates queue and exit",
    ),
    title_contains: Optional[str] = typer.Option(
        None,
        "--title-contains",
        help="Only analyze jobs whose title contains this string (case-insensitive)",
    ),
    location_contains: Optional[str] = typer.Option(
        None,
        "--location-contains",
        help="Only analyze jobs whose location contains this string (case-insensitive)",
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
        try:
            analyzer.init_db(conn)

            if candidates:
                analyzer.print_candidates(conn)
                return

            if promote is not None and not file and not all_files:
                analyzer.apply_candidates(conn, promote)
                return

            args = argparse.Namespace(file=str(file) if file else None, all=all_files)
            paths = analyzer.resolve_input_paths(args, data_dir)
            if paths is None:
                if promote is not None:
                    analyzer.apply_candidates(conn, promote)
                return

            _run_analysis_pipeline(
                analyzer,
                conn,
                paths,
                data_dir,
                promote,
                llm,
                title_contains,
                location_contains,
            )
        finally:
            conn.close()
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
