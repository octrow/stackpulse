import sqlite3
from collections import Counter
from datetime import date

from config import LLM_CANDIDATE_THRESHOLD
from analysis_db import normalize_term
from ui_rich import (
    console,
    is_compact,
    make_table,
    metric_title,
    print_info,
    print_panel,
    print_success,
    print_warning,
)

# ── Candidate promotion constants ─────────────────────────────────────────────

# Generic terms that should never enter the skills catalog regardless of frequency
SKIP_TERMS: set[str] = {
    "api",
    "testing",
    "automation",
    "debugging",
    "configuration",
    "scalability",
    "concurrency",
    "orchestration",
    "containerization",
    "profiling",
    "modular code",
    "testable code",
    "object-oriented programming",
    "async programming",
    "asynchronous programming",
    "error diagnosis",
    "database optimization",
    "memory optimization",
    "orm optimization",
    "restful apis",
    "rest apis",
    "ci/cd workflows",
    "ci/cd pipelines",
    "agentic systems",
    "autonomous ai agents",
}


def promote_llm_to_candidates(
    conn: sqlite3.Connection, threshold: int = LLM_CANDIDATE_THRESHOLD
) -> int:
    """Aggregate llm_results and queue new terms in skill_candidates.

    Only terms seen in >= threshold distinct jobs are added.
    Terms already in skills or in candidates (any status) are skipped.
    Existing candidates get their jobs_count updated if it has grown.

    Returns the number of newly added candidates.
    """
    existing_skill_terms: set[str] = {
        row["term"] for row in conn.execute("SELECT term FROM skills")
    }
    existing_alias_terms: set[str] = {
        row["canonical"] for row in conn.execute("SELECT canonical FROM skill_aliases")
    }
    known_terms = existing_skill_terms | existing_alias_terms

    # Existing candidates keyed by (term, category_id) → current jobs_count
    existing_candidates: dict[tuple[str, int], int] = {
        (row["term"], row["category_id"]): row["jobs_count"]
        for row in conn.execute(
            "SELECT term, category_id, jobs_count FROM skill_candidates"
        )
    }

    llm_rows = conn.execute(
        """
        SELECT r.skill, c.name AS category, r.category_id,
               COUNT(DISTINCT r.url_key) AS n
        FROM llm_results r
        JOIN categories c ON r.category_id = c.id
        WHERE r.is_matched = 0
        GROUP BY r.skill, r.category_id
        HAVING n >= ?
        ORDER BY n DESC
    """,
        (threshold,),
    ).fetchall()

    newly_added_count = updated_count = 0
    today = date.today().isoformat()

    for llm_row in llm_rows:
        skill: str = llm_row["skill"]
        category_id: int = llm_row["category_id"]
        jobs_count: int = llm_row["n"]

        normalized = normalize_term(skill)

        if normalized in SKIP_TERMS:
            continue

        if normalized in known_terms:
            continue

        candidate_key = (normalized, category_id)

        if candidate_key in existing_candidates:
            if jobs_count > existing_candidates[candidate_key]:
                conn.execute(
                    "UPDATE skill_candidates SET jobs_count=? WHERE term=? AND category_id=?",
                    (jobs_count, normalized, category_id),
                )
                updated_count += 1
        else:
            conn.execute(
                """INSERT OR IGNORE INTO skill_candidates
                   (term, category_id, llm_category_id, jobs_count, added_date)
                   VALUES (?,?,?,?,?)""",
                (normalized, category_id, category_id, jobs_count, today),
            )
            newly_added_count += 1
            existing_candidates[candidate_key] = jobs_count

    conn.commit()

    pending_count = conn.execute(
        "SELECT COUNT(*) FROM skill_candidates WHERE status='pending'"
    ).fetchone()[0]
    print_info(
        f"[Candidates queue] {newly_added_count} new, {updated_count} updated "
        f"(threshold >=\u2009{threshold} jobs) → {pending_count} pending review rows"
    )
    return newly_added_count


def get_pending_candidates(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return pending skill_candidates rows with category name, ordered for review."""
    return conn.execute(
        """SELECT sc.term, c.name AS category, sc.category_id, sc.jobs_count
           FROM skill_candidates sc
           JOIN categories c ON sc.category_id = c.id
           WHERE sc.status = 'pending'
           ORDER BY sc.jobs_count DESC, sc.term"""
    ).fetchall()


def approve_candidate(conn: sqlite3.Connection, term: str, category_id: int) -> bool:
    """Insert term into skills and mark the pending candidate approved. Returns False if no matching pending row."""
    row = conn.execute(
        """SELECT 1 FROM skill_candidates
           WHERE term = ? AND category_id = ? AND status = 'pending'""",
        (term, category_id),
    ).fetchone()
    if not row:
        return False
    today = date.today().isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO skills(category_id, term) VALUES (?,?)",
        (category_id, term),
    )
    conn.execute(
        """UPDATE skill_candidates
           SET status = 'approved', decided_date = ?
           WHERE term = ? AND category_id = ?""",
        (today, term, category_id),
    )
    return True


def reject_candidate(conn: sqlite3.Connection, term: str, category_id: int) -> bool:
    """Mark a pending candidate rejected. Returns False if no matching pending row."""
    today = date.today().isoformat()
    cur = conn.execute(
        """UPDATE skill_candidates
           SET status = 'rejected', decided_date = ?
           WHERE term = ? AND category_id = ? AND status = 'pending'""",
        (today, term, category_id),
    )
    return cur.rowcount > 0


def apply_candidates(conn: sqlite3.Connection, min_jobs: int = 2) -> int:
    """Promote pending candidates with jobs_count >= min_jobs into skills catalog.

    Returns the number of terms promoted.
    """
    candidate_rows = conn.execute(
        """SELECT sc.term, c.name AS category, sc.category_id, sc.jobs_count
           FROM skill_candidates sc
           JOIN categories c ON sc.category_id = c.id
           WHERE sc.status='pending' AND sc.jobs_count >= ?
           ORDER BY sc.jobs_count DESC""",
        (min_jobs,),
    ).fetchall()

    if not candidate_rows:
        print_warning(f"[Promote] No pending candidates with jobs_count >= {min_jobs}.")
        return 0

    print_panel(
        "Promote Candidates",
        [
            f"Adding {len(candidate_rows)} terms to skills",
            f"Threshold: >= {min_jobs} jobs",
        ],
        style="cyan",
    )

    table = make_table(metric_title("Candidates to promote"))
    table.add_column("Term", style="bold", no_wrap=is_compact(), max_width=24)
    table.add_column("Category", overflow="fold", max_width=28)
    table.add_column("Jobs", justify="right", width=6)

    for row in candidate_rows:
        table.add_row(row["term"], row["category"], str(row["jobs_count"]))
        approve_candidate(conn, row["term"], row["category_id"])

    console.print(table)
    conn.commit()
    print_success(f"Promoted {len(candidate_rows)} term(s).")
    return len(candidate_rows)


def print_candidates(conn: sqlite3.Connection) -> None:
    """Print the skill_candidates queue grouped by status."""
    rows = conn.execute(
        """SELECT sc.term, c.name AS category, lc.name AS llm_category,
                  sc.jobs_count, sc.status
           FROM skill_candidates sc
           JOIN categories c  ON sc.category_id     = c.id
           JOIN categories lc ON sc.llm_category_id = lc.id
           ORDER BY
               CASE sc.status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
               sc.jobs_count DESC"""
    ).fetchall()

    if not rows:
        print_warning("No skill candidates found. Run: py analyze.py --llm")
        return

    status_counts: Counter = Counter(row["status"] for row in rows)
    print_panel(
        "Skill Candidates Queue",
        [
            f"Total: {len(rows)}",
            f"Pending: {status_counts.get('pending', 0)}",
            f"Approved: {status_counts.get('approved', 0)}",
            f"Rejected: {status_counts.get('rejected', 0)}",
        ],
        style="cyan",
    )
    print_info(
        "Queue summary is separate from skills/alias coverage gaps shown in --llm output."
    )

    pending_rows = [row for row in rows if row["status"] == "pending"]
    if pending_rows:
        table = make_table("Pending candidates", expand=True)
        table.add_column("Term", style="bold")
        table.add_column("Category", overflow="fold")
        table.add_column("LLM category", overflow="fold")
        table.add_column("Jobs", justify="right")
        for row in pending_rows:
            table.add_row(
                row["term"],
                row["category"],
                row["llm_category"],
                str(row["jobs_count"]),
            )
        console.print(table)

    print_info("To promote all pending (>= 2 jobs): py analyze.py --promote")
    print_info(
        "To reject a term: sqlite3 data/skills.db \"UPDATE skill_candidates SET status='rejected' WHERE term='<term>'\""
    )
