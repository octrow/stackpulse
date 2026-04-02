from __future__ import annotations
from typing import Coroutine, Optional, TypeVar

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path
import typer
from rich.traceback import install as install_rich_traceback
from config import LLM_CANDIDATE_THRESHOLD
from ui_rich import (
    console,
    make_table,
    print_error,
    print_info,
    print_panel,
    print_section,
    print_success,
    print_warning,
    set_display_mode,
)

T = TypeVar("T")

install_rich_traceback(show_locals=False)

app = typer.Typer(help="StackPulse CLI", invoke_without_command=True)


@app.callback(invoke_without_command=True)
def default_command(ctx: typer.Context) -> None:
    """Run interactive wizard when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        _interactive_wizard()


def _interactive_wizard() -> None:
    """Prompt the user for what to run when stackpulse is called with no args."""
    print_section("StackPulse")
    print_info("What would you like to do?")
    console.print()
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


def _ensure_venv(venv_path: Path, python: str, repo_root: Path) -> tuple[str, str, str]:
    """Create virtualenv if missing and return summary row tuple."""
    venv_python = _venv_python(venv_path)
    if venv_python.exists():
        return ("Create venv", "[yellow]skip[/yellow]", f"exists: {venv_path}")

    _run_command(
        [python, "-m", "venv", str(venv_path)],
        "Creating virtual environment",
        cwd=repo_root,
    )
    return ("Create venv", "[green]done[/green]", str(venv_path))


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


def _ensure_dependencies(venv_python: Path, repo_root: Path) -> tuple[str, str, str]:
    """Install requirements when missing and return summary row tuple."""
    if _deps_installed(venv_python):
        return (
            "Install dependencies",
            "[yellow]skip[/yellow]",
            "requirements already satisfied",
        )

    _run_command(
        [str(venv_python), "-m", "pip", "install", "-r", "requirements.txt"],
        "Installing dependencies",
        cwd=repo_root,
    )
    return (
        "Install dependencies",
        "[green]done[/green]",
        "pip install -r requirements.txt",
    )


def _chromium_installed() -> bool:
    cache_dir = Path.home() / ".cache" / "ms-playwright"
    if not cache_dir.exists():
        return False
    return any(path.name.startswith("chromium-") for path in cache_dir.iterdir())


def _ensure_chromium(venv_python: Path, repo_root: Path) -> tuple[str, str, str]:
    """Install Playwright Chromium when missing and return summary row tuple."""
    if _chromium_installed():
        return (
            "Install Chromium",
            "[yellow]skip[/yellow]",
            "Playwright Chromium already present",
        )

    _run_command(
        [str(venv_python), "-m", "playwright", "install", "chromium"],
        "Installing Playwright Chromium",
        cwd=repo_root,
    )
    return ("Install Chromium", "[green]done[/green]", "playwright install chromium")


def _ensure_session(
    venv_python: Path, repo_root: Path, session_file: str
) -> tuple[str, str, str]:
    """Run session setup only when session file is missing."""
    session_path = repo_root / session_file
    if session_path.exists():
        return ("Setup session", "[yellow]skip[/yellow]", f"exists: {session_file}")

    print_info("Session file missing. Starting interactive setup-session...")
    subprocess.run([str(venv_python), "setup_session.py"], cwd=repo_root, check=True)
    return ("Setup session", "[green]done[/green]", session_file)


def _show_auto_summary(rows: list[tuple[str, str, str]]) -> None:
    table = make_table("Auto workflow summary", expand=True)
    table.add_column("Step", style="bold")
    table.add_column("Status")
    table.add_column("Details", overflow="fold")
    for step, status, details in rows:
        table.add_row(step, status, details)
    console.print(table)
    print_success("Auto workflow summary complete.")


@app.command("setup-session")
def setup_session_command() -> None:
    """Create or refresh LinkedIn session.json."""
    try:
        from setup_session import main as setup_session_main

        with console.status(
            "[bold cyan]Running LinkedIn session setup...", spinner="dots"
        ):
            asyncio.run(setup_session_main())
        print_success("Session setup complete.")
    except Exception as exc:  # noqa: BLE001
        print_error(f"setup-session failed: {exc}")
        raise typer.Exit(code=1) from exc


def _run_async(coro: Coroutine[object, object, T]) -> T:
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
        print_success("Scrape completed.")
    except KeyboardInterrupt as exc:
        print_warning("Scrape interrupted by user.")
        raise typer.Exit(code=130) from exc
    except Exception as exc:  # noqa: BLE001
        print_error(f"scrape failed: {exc}")
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
    print_section("Analyze Pipeline")
    print_info("Preparing analysis context")

    if promote is not None:
        analyzer.apply_candidates(conn, promote)

    skills = analyzer.load_skills(conn)
    term_count = sum(len(terms) for terms in skills.values())

    jobs = analyzer.load_jobs(paths)

    summary = make_table("Run Summary", expand=True)
    summary.add_column("Metric", style="bold")
    summary.add_column("Value")
    summary.add_row("Input files", str(len(paths)))
    summary.add_row("Unique jobs loaded", str(len(jobs)))
    summary.add_row("Skills catalog terms", str(term_count))
    summary.add_row("Skill categories", str(len(skills)))
    summary.add_row("LLM mode", "enabled" if use_llm else "disabled")
    console.print(summary)

    if title_contains:
        jobs = [
            j
            for j in jobs
            if title_contains.lower() in (j.get("job_title") or "").lower()
        ]
        print_info(f"Title filter '{title_contains}' → {len(jobs)} jobs")
    if location_contains:
        jobs = [
            j
            for j in jobs
            if location_contains.lower() in (j.get("location") or "").lower()
        ]
        print_info(f"Location filter '{location_contains}' → {len(jobs)} jobs")

    if not jobs:
        print_warning("No jobs left after filters. Nothing to analyze.")
        return

    llm_client = None
    if use_llm:
        llm_client = analyzer.build_llm_client(
            analyzer.NINEROUTER_BASE_URL,
            analyzer.NINEROUTER_MODEL,
            analyzer.NINEROUTER_API_KEY,
        )

    with console.status(
        "[bold cyan]Analyzing jobs and extracting skills...", spinner="dots"
    ):
        df = analyzer.analyze(jobs, skills, llm_client=llm_client, conn=conn)

    if use_llm and llm_client:
        analyzer.promote_llm_to_candidates(conn, threshold=LLM_CANDIDATE_THRESHOLD)

    existing_candidate_terms = {
        row[0] for row in conn.execute("SELECT term FROM skill_candidates")
    }

    analyzer.print_report(df, skills, existing_candidate_terms, LLM_CANDIDATE_THRESHOLD)

    output_stem = paths[0].stem if len(paths) == 1 else "jobs_all"
    output_path = data_dir / f"{output_stem}_analysis.xlsx"
    analyzer.save_excel(df, output_path, skills)
    print_panel(
        "Analyze Completed",
        [
            f"Rows analyzed: {len(df)}",
            f"Export: {output_path}",
        ],
        style="green",
    )
    print_success("Analysis completed.")


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
    view: str = typer.Option(
        "detailed",
        "--view",
        help="Output density mode: detailed or compact",
        case_sensitive=False,
    ),
) -> None:
    """Analyze scraped jobs and export Excel output."""
    if file and all_files:
        print_error("Use either --file or --all, not both.")
        raise typer.Exit(code=1)

    try:
        import analyze as analyzer

        set_display_mode(view.lower())

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
        print_error(f"analyze failed: {exc}")
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
        rows.append(_ensure_venv(venv_path, python, repo_root))

        rows.append(_ensure_dependencies(venv_python, repo_root))
        rows.append(_ensure_chromium(venv_python, repo_root))

        from config import JOBS_PER_QUERY, SESSION_FILE

        rows.append(_ensure_session(venv_python, repo_root, SESSION_FILE))

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
        print_success("Auto workflow completed.")
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
            print_error("Invalid LLM flag usage. Use: stackpulse analyze --llm")
            sys.exit(2)

    app()


if __name__ == "__main__":
    main()
