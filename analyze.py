"""
Skills analysis on collected job JSON files.

Usage:
    python analyze.py                        # analyze today's file
    python analyze.py --file data/jobs_2026-04-01.json
    python analyze.py --all                  # merge all JSON files in data/
    python analyze.py --llm                  # also extract skills via LLM (free, via 9router)
    python analyze.py --all --llm
    python analyze.py --promote              # promote LLM-discovered candidates (≥2 jobs) into skills
    python analyze.py --promote 3            # same but threshold = 3 jobs
    python analyze.py --all --promote        # promote first, then analyze with enriched skills
    python analyze.py --candidates           # show pending skill candidates queue

DB: data/skills.db stores skills catalog + LLM results (auto-created on first run).
To add a term without touching code:
    sqlite3 data/skills.db "INSERT OR IGNORE INTO skills(category_id,term) SELECT id,'hetzner' FROM categories WHERE name='Cloud'"
To reject a candidate so it never gets promoted:
    sqlite3 data/skills.db "UPDATE skill_candidates SET status='rejected' WHERE term='<term>'"
To add an alias (e.g. German synonym):
    sqlite3 data/skills.db \\
      "INSERT INTO skill_aliases(skill_id,alias,canonical,lang,alias_type)
       SELECT id,'Deutsch','deutsch','de','translation' FROM skills WHERE term='german'"
"""

import argparse
import json
import re
import sqlite3
import time
from argparse import Namespace
from collections import Counter
from datetime import date
from pathlib import Path
import pandas as pd

from ui_rich import (
    console,
    make_table,
    percent_bar,
    print_info,
    print_panel,
    print_section,
    print_success,
)

from config import (
    OUTPUT_DIR,
    NINEROUTER_BASE_URL,
    NINEROUTER_MODEL,
    NINEROUTER_FALLBACK_MODEL,
    NINEROUTER_API_KEY,
    LLM_RATE_LIMIT_MAX_WAIT_SECONDS,
    LLM_MAX_INPUT_CHARS,
    LLM_MAX_OUTPUT_TOKENS,
    RETRY_AFTER_BUFFER_SECONDS,
    LLM_CANDIDATE_THRESHOLD,
)
from analysis_candidates import (
    SKIP_TERMS,
    apply_candidates,
    print_candidates,
    promote_llm_to_candidates,
)
from analysis_db import (
    canonical_linkedin_job_key,
    init_db,
    load_skills,
    normalize_term,
    open_db,
)
from analysis_llm_cache import _llm_cache_get, _llm_cache_set, _url_key


# ── Display formatting constants ──────────────────────────────────────────────

_REPORT_TOP_SKILLS_COUNT = 30
_REPORT_TOP_CATEGORIES_COUNT = 8
_REPORT_TOP_LOCATIONS_COUNT = 15
_REPORT_TOP_SALARY_COUNT = 20
_REPORT_TOP_MISSING_SKILLS_COUNT = 50


# ── LLM extraction ────────────────────────────────────────────────────────────


def _build_llm_prompt(skills: dict[str, list[tuple[str, str]]]) -> str:
    """Build a skills-aware LLM prompt.

    Serializes existing skill terms grouped by category so the LLM can match
    against known terms and only flag genuinely new discoveries.
    """
    # Deduplicate display terms per category (aliases share the same display)
    lines = []
    categories_list = []
    for category, term_pairs in skills.items():
        unique_terms = sorted({display for display, _ in term_pairs})
        lines.append(f"  {category}: {', '.join(unique_terms)}")
        categories_list.append(category)

    skills_block = "\n".join(lines)
    categories_block = ", ".join(f'"{c}"' for c in categories_list)

    return f"""You are a skill extraction assistant. Below is a catalog of known technical skills grouped by category.

KNOWN SKILLS:
{skills_block}

TASK: Analyze the job description and:
1. List ALL known skill terms that are mentioned or clearly implied. Use the EXACT term from the catalog — do not paraphrase.
2. List any genuinely NEW technical skills/tools/protocols NOT in the catalog. For each, use the EXACT category name from this list — copy verbatim: {categories_block}.

Return ONLY JSON:
{{
  "matched": ["term1", "term2"],
  "new_terms": [{{"term": "newterm", "category": "Category Name"}}]
}}

Rules:
- "matched" must contain ONLY exact terms from the catalog above, lowercase
- "new_terms": only include specific, concrete technologies, tools, libraries, or protocols — NOT generic concepts like "debugging", "scalability", "containerization", "restful apis", "ci/cd pipelines", "async programming", "design patterns"
- Do NOT include soft skills or company names
- Return ONLY the JSON, no explanation

Job description:
"""


def _parse_retry_after(error_message: str) -> int | None:
    """Parse the suggested wait time (seconds) from a 429 error message.

    Handles both:
      "Please try again in 18m0.864s"  (groq TPD exhaustion)
      "reset after 1m 4s"              (per-minute window reset)
    Returns seconds rounded up plus RETRY_AFTER_BUFFER_SECONDS, or None if unparseable.
    """
    for pattern in (
        r"try again in (?:(\d+)m\s*)?(\d+(?:\.\d+)?)s",
        r"reset after (?:(\d+)m\s*)?(\d+(?:\.\d+)?)s",
    ):
        match = re.search(pattern, str(error_message))
        if match:
            minutes = int(match.group(1)) if match.group(1) else 0
            seconds = float(match.group(2))
            return int(minutes * 60 + seconds) + RETRY_AFTER_BUFFER_SECONDS
    return None


def _llm_call(client, model: str, prompt: str, text: str) -> dict:
    """Execute a single LLM extraction call and return parsed JSON.

    Raises on any error — callers handle retries and fallback.
    """
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt + text[:LLM_MAX_INPUT_CHARS]}],
        max_tokens=LLM_MAX_OUTPUT_TOKENS,
        temperature=0,
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def _call_llm_with_retry(
    client,
    model: str,
    prompt: str,
    text: str,
    url: str,
    attempt_label: str,
) -> dict | None:
    """Execute one LLM call with at most one retry on short 429 windows."""
    import json as _json

    from openai import APIConnectionError, APIError, RateLimitError

    try:
        return _llm_call(client, model, prompt, text)
    except RateLimitError as rate_limit_error:
        wait_seconds = _parse_retry_after(str(rate_limit_error))
        if wait_seconds is not None and wait_seconds <= LLM_RATE_LIMIT_MAX_WAIT_SECONDS:
            print(
                f"  [LLM] 429 on {attempt_label} — sleeping {wait_seconds}s then retrying …"
            )
            time.sleep(wait_seconds)
            try:
                return _llm_call(client, model, prompt, text)
            except RateLimitError as retry_error:
                print(f"  [LLM] 429 again after retry ({attempt_label}): {retry_error}")
                return None

        wait_display = f"{wait_seconds}s" if wait_seconds else "unknown"
        print(
            f"  [LLM] 429 on {attempt_label} — wait {wait_display} exceeds limit, "
            f"skipping primary"
        )
        return None
    except (APIError, APIConnectionError, _json.JSONDecodeError, ValueError) as error:
        print(f"  [LLM] Warning: extraction failed for {url[:60]}: {error}")
        return None


def _extract_skills_with_models(
    text: str,
    url: str,
    client,
    prompt: str,
) -> dict | None:
    """Run primary model, then optional fallback model on failure."""
    result = _call_llm_with_retry(
        client,
        NINEROUTER_MODEL,
        prompt,
        text,
        url,
        NINEROUTER_MODEL,
    )
    if result is not None:
        return result

    if not NINEROUTER_FALLBACK_MODEL:
        return None

    print(f"  [LLM] Trying fallback model: {NINEROUTER_FALLBACK_MODEL}")
    return _call_llm_with_retry(
        client,
        NINEROUTER_FALLBACK_MODEL,
        prompt,
        text,
        url,
        f"fallback:{NINEROUTER_FALLBACK_MODEL}",
    )


def _normalize_llm_result(
    raw_result: dict, skills: dict[str, list[tuple[str, str]]]
) -> dict[str, list[str]]:
    """Convert LLM output to internal cache format.

    LLM returns: {"matched": ["python", ...], "new_terms": [{"term": "x", "category": "Y"}, ...]}
    Internal: {"_matched": ["python", ...], "Category Name": ["new_term", ...]}

    The "_matched" key is an internal marker — _llm_cache_set translates it to
    is_matched=1 with the actual skills category when writing to DB.
    """
    if "matched" not in raw_result and "new_terms" not in raw_result:
        # Old-format LLM result — treat all as new discoveries
        return raw_result

    # Build set of valid skill display terms for validation
    valid_terms = {display for terms in skills.values() for display, _ in terms}

    normalized: dict[str, list[str]] = {}

    # Matched terms — validate against known skills
    matched = raw_result.get("matched", [])
    if matched:
        validated = [
            t for t in matched if isinstance(t, str) and t.lower() in valid_terms
        ]
        if validated:
            normalized["_matched"] = validated

    # New terms — store under their suggested category
    new_terms = raw_result.get("new_terms", [])
    for entry in new_terms:
        if not isinstance(entry, dict):
            continue
        term = entry.get("term", "")
        category = entry.get("category", "")
        if term and category:
            normalized.setdefault(category, []).append(term.lower())

    return normalized


def extract_skills_llm(
    text: str,
    url: str,
    conn: sqlite3.Connection,
    client,
    prompt: str = "",
    taxonomy: dict[str, list[tuple[str, str]]] | None = None,
) -> dict[str, list[str]]:
    """Call LLM via 9router to extract skills. Uses DB cache to avoid re-calls.

    On 429:
      - If the suggested wait is ≤ LLM_RATE_LIMIT_MAX_WAIT_SECONDS, sleeps and retries once.
      - If NINEROUTER_FALLBACK_MODEL is configured, tries that next.
      - Otherwise logs a warning and returns {}.
    """
    cache_key = _url_key(url)
    cached = _llm_cache_get(conn, cache_key)
    if cached is not None:
        return cached

    result = _extract_skills_with_models(text, url, client, prompt)
    if result is None:
        return {}

    # Normalize LLM output before caching
    if taxonomy is not None:
        result = _normalize_llm_result(result, taxonomy)

    _llm_cache_set(conn, url, cache_key, result)
    return result


# ── Regex-based taxonomy extraction ──────────────────────────────────────────


def extract_skills(
    text: str, taxonomy: dict[str, list[tuple[str, str]]]
) -> dict[str, list[str]]:
    """Match taxonomy terms and aliases against text.

    taxonomy format: {category: [(display_term, regex_pattern), ...]}
    Returns {category: [display_term, ...]} — aliases resolve to their canonical display term,
    duplicates within a category are suppressed.
    """
    text_lower = text.lower()
    matched_skills: dict[str, list[str]] = {}
    for category, term_pairs in taxonomy.items():
        deduplicated_displays: set[str] = set()
        category_hits: list[str] = []
        for display, pattern in term_pairs:
            if display not in deduplicated_displays and re.search(
                r"\b" + pattern + r"\b", text_lower
            ):
                deduplicated_displays.add(display)
                category_hits.append(display)
        if category_hits:
            matched_skills[category] = category_hits
    return matched_skills


# ── Data loading ──────────────────────────────────────────────────────────────


def load_jobs(paths: list[Path]) -> list[dict]:
    """Load and deduplicate jobs from one or more JSON files."""
    all_jobs = []
    for path in paths:
        with open(path, encoding="utf-8") as file_handle:
            all_jobs.extend(json.load(file_handle))
    seen_url_keys: set[str] = set()
    deduplicated_jobs = []
    for job in all_jobs:
        url_key = canonical_linkedin_job_key(job.get("linkedin_url"))
        if not url_key:
            deduplicated_jobs.append(job)
            continue
        if url_key in seen_url_keys:
            continue
        seen_url_keys.add(url_key)
        deduplicated_jobs.append(job)
    return deduplicated_jobs


# ── Analysis ──────────────────────────────────────────────────────────────────


def _build_comprehensive_by_category(
    skills_found: dict[str, list[str]],
    llm_skills: dict[str, list[str]],
    skills_catalog: dict[str, list[tuple[str, str]]],
) -> dict[str, list[str]]:
    """Merge regex hits with LLM skills into unified per-category dict.

    Matched LLM terms (under '_matched' key) are routed to their catalog category
    via reverse lookup. New discoveries are stored under their LLM-suggested category.
    """
    merged: dict[str, list[str]] = {
        cat: list(hits) for cat, hits in skills_found.items()
    }

    if not llm_skills:
        return merged

    # Build reverse index: display_term -> category
    term_to_cat: dict[str, str] = {}
    for cat, term_pairs in skills_catalog.items():
        for display, _ in term_pairs:
            term_to_cat.setdefault(display, cat)

    for llm_cat, skills in llm_skills.items():
        for skill in skills:
            if llm_cat == "_matched":
                target_cat = term_to_cat.get(skill.lower())
                if target_cat is None:
                    continue
            else:
                target_cat = llm_cat
            existing = merged.setdefault(target_cat, [])
            if skill.lower() not in {s.lower() for s in existing}:
                existing.append(skill)

    return merged


def analyze(
    jobs: list[dict],
    taxonomy: dict[str, list[tuple[str, str]]],
    llm_client=None,
    conn: sqlite3.Connection | None = None,
) -> pd.DataFrame:
    """Build a DataFrame with per-job metadata and extracted skills."""
    # Build skills-aware prompt once for the entire run
    llm_prompt = _build_llm_prompt(taxonomy) if llm_client else ""

    job_rows = []
    for job in jobs:
        description = job.get("job_description") or ""
        title = job.get("job_title") or ""
        combined_text = f"{title} {description}"
        url = job.get("linkedin_url", "")

        skills_found = extract_skills(combined_text, taxonomy)

        llm_skills: dict[str, list[str]] = {}
        if llm_client and conn is not None:
            llm_skills = extract_skills_llm(
                combined_text,
                url,
                conn,
                llm_client,
                prompt=llm_prompt,
                taxonomy=taxonomy,
            )

        # Unified per-category dict (regex + LLM merged)
        skills_by_cat = _build_comprehensive_by_category(
            skills_found,
            llm_skills,
            taxonomy,
        )

        regex_skills_flat = [skill for hits in skills_found.values() for skill in hits]
        all_skills_comprehensive = [
            skill for hits in skills_by_cat.values() for skill in hits
        ]

        job_rows.append(
            {
                "job_title": job.get("job_title"),
                "company": job.get("company"),
                "location": job.get("location"),
                "search_location": job.get("search_location"),
                "posted_date": job.get("posted_date"),
                "salary_extracted": job.get("salary_extracted"),
                "linkedin_url": url,
                "scraped_date": job.get("scraped_date"),
                "applicant_count": job.get("applicant_count"),
                "skills_raw": skills_found,
                "skills_by_category": skills_by_cat,
                "all_skills_flat": regex_skills_flat,
                "all_skills_comprehensive": all_skills_comprehensive,
                "skills_llm": llm_skills,
                "has_description": bool(description.strip()),
            }
        )

    return pd.DataFrame(job_rows)


def _print_top_skills(df: pd.DataFrame) -> None:
    """Print top skill frequencies across all postings.

    Uses the comprehensive column (regex + LLM merged) when available.
    """
    col = (
        "all_skills_comprehensive"
        if "all_skills_comprehensive" in df.columns
        else "all_skills_flat"
    )
    all_skills: Counter = Counter()
    for skills_list in df[col]:
        all_skills.update(skills_list)

    has_llm = "skills_llm" in df.columns and df["skills_llm"].apply(bool).any()
    label = (
        "comprehensive (regex + LLM)"
        if col == "all_skills_comprehensive" and has_llm
        else "regex taxonomy"
    )

    table = make_table(f"Top {_REPORT_TOP_SKILLS_COUNT} skills [{label}]", expand=True)
    table.add_column("Skill", style="bold")
    table.add_column("Jobs", justify="right")
    table.add_column("Prevalence", justify="right")
    table.add_column("Signal")

    for skill, count in all_skills.most_common(_REPORT_TOP_SKILLS_COUNT):
        percentage = count / len(df) * 100
        table.add_row(skill, str(count), f"{percentage:.1f}%", percent_bar(percentage))

    console.print(table)


def _print_category_breakdown(
    df: pd.DataFrame,
    taxonomy: dict[str, list[tuple[str, str]]],
) -> None:
    """Print category-wise top terms (regex + LLM when available)."""
    col = "skills_by_category" if "skills_by_category" in df.columns else "skills_raw"
    skills_list: list[dict] = df[col].tolist()

    table = make_table("By category", expand=True)
    table.add_column("Category", style="bold", overflow="fold")
    table.add_column("Top terms", overflow="fold")

    # Collect all categories present across jobs (taxonomy order first, then extras)
    all_categories: list[str] = list(taxonomy.keys())
    extra = {cat for row in skills_list for cat in row if cat not in taxonomy}
    all_categories.extend(sorted(extra))

    for category in all_categories:
        category_counter: Counter = Counter()
        for skills_row in skills_list:
            if category in skills_row:
                category_counter.update(skills_row[category])
        if not category_counter:
            continue

        total_jobs = len(df)
        top_terms = ", ".join(
            f"{term}({count}, {count / total_jobs * 100:.0f}%)"
            for term, count in category_counter.most_common(
                _REPORT_TOP_CATEGORIES_COUNT
            )
        )
        table.add_row(category, top_terms)

    console.print(table)


def _print_top_locations(df: pd.DataFrame) -> None:
    """Print the most frequent locations in scraped results."""
    table = make_table("Top locations in results", expand=True)
    table.add_column("Location", style="bold", overflow="fold")
    table.add_column("Jobs", justify="right")
    for location, count in (
        df["location"].value_counts().head(_REPORT_TOP_LOCATIONS_COUNT).items()
    ):
        table.add_row(str(location), str(count))
    console.print(table)


def _print_salary_hints(df: pd.DataFrame) -> None:
    """Print postings where a salary hint was extracted."""
    salary_rows = df[df["salary_extracted"].notna()]
    table = make_table(
        f"Salary hints found in {len(salary_rows)}/{len(df)} postings", expand=True
    )
    table.add_column("Role / Company", style="bold", overflow="fold")
    table.add_column("Location", overflow="fold")
    table.add_column("Salary hint", overflow="fold")

    for _, row in salary_rows.head(_REPORT_TOP_SALARY_COUNT).iterrows():
        label = row["job_title"] or row.get("company") or "N/A"
        location = row.get("search_location") or row.get("location") or ""
        table.add_row(str(label), str(location), str(row["salary_extracted"]))

    console.print(table)


def _known_skill_terms(skills: dict[str, list[tuple[str, str]]]) -> set[str]:
    """Return normalized skill terms (including aliases) for membership checks."""
    return {display for terms in skills.values() for display, _ in terms}


def _count_missing_skill_terms(
    skills_llm_list: list[dict],
    known_terms: set[str],
) -> Counter:
    """Count LLM terms absent from current skills/alias coverage.

    Only non-matched entries (new discoveries) are considered missing.
    """
    missing: Counter = Counter()
    for llm_skills in skills_llm_list:
        if not llm_skills:
            continue
        for cat, skills in llm_skills.items():
            if cat == "_matched":
                continue
            for skill in skills:
                normalized = normalize_term(skill)
                if normalized not in known_terms:
                    missing[normalized] += 1
    return missing


def _build_actionable_missing_terms(
    skills_missing: Counter,
    existing_candidate_terms: set[str],
    threshold: int,
) -> Counter:
    """Filter uncovered terms to queue-actionable terms."""
    skip_normalized = {normalize_term(term) for term in SKIP_TERMS}
    return Counter(
        {
            term: count
            for term, count in skills_missing.items()
            if count >= threshold
            and term not in skip_normalized
            and term not in existing_candidate_terms
        }
    )


def _print_missing_skill_terms(
    skills_missing: Counter,
    actionable_missing: Counter,
    threshold: int,
) -> None:
    """Print uncovered LLM terms and queue-actionable subset."""
    if not skills_missing:
        return

    print_panel(
        "Coverage gap summary",
        [
            f"Uncovered terms: {len(skills_missing)}",
            (
                "Actionable for queue "
                f"(threshold >= {threshold}, not SKIP_TERMS, not already queued): "
                f"{len(actionable_missing)}"
            ),
        ],
        style="yellow",
    )

    table = make_table("Top uncovered terms", expand=True)
    table.add_column("Term", style="bold")
    table.add_column("Jobs", justify="right")
    for skill, count in skills_missing.most_common(_REPORT_TOP_MISSING_SKILLS_COUNT):
        table.add_row(skill, str(count))
    console.print(table)

    if actionable_missing:
        actionable_table = make_table("Top actionable uncovered terms", expand=True)
        actionable_table.add_column("Term", style="bold")
        actionable_table.add_column("Jobs", justify="right")
        for skill, count in actionable_missing.most_common(
            _REPORT_TOP_MISSING_SKILLS_COUNT
        ):
            actionable_table.add_row(skill, str(count))
        console.print(actionable_table)

    print_info("Raw uncovered count is broader than pending queue by design.")


def _print_llm_section(
    df: pd.DataFrame,
    skills_catalog: dict[str, list[tuple[str, str]]],
    existing_candidate_terms: set[str],
    candidate_threshold: int,
) -> None:
    """Print skills coverage gaps discovered by LLM extraction."""
    if "skills_llm" not in df.columns or not df["skills_llm"].apply(bool).any():
        return

    print_section("Skills Coverage Gaps (LLM-discovered)")

    skills_llm_list: list[dict] = df["skills_llm"].tolist()

    known_terms = _known_skill_terms(skills_catalog)
    skills_missing = _count_missing_skill_terms(
        skills_llm_list,
        known_terms,
    )
    actionable_missing = _build_actionable_missing_terms(
        skills_missing,
        existing_candidate_terms,
        candidate_threshold,
    )
    _print_missing_skill_terms(
        skills_missing,
        actionable_missing,
        candidate_threshold,
    )


def _print_quality_summary(df: pd.DataFrame) -> None:
    """Print extraction quality counters."""
    total = len(df)
    no_desc = (~df["has_description"]).sum() if "has_description" in df.columns else 0
    no_skills = (df["all_skills_flat"].apply(len) == 0).sum()
    if total == 0:
        table = make_table("Extraction quality")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        table.add_row("Empty description", "0 (0.0%)")
        table.add_row("Zero skills found", "0 (0.0%)")
        console.print(table)
        return

    table = make_table(f"Extraction quality ({total} jobs)")
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Percent", justify="right")
    table.add_row("Empty description", str(no_desc), f"{no_desc / total * 100:.1f}%")
    table.add_row(
        "Zero skills found", str(no_skills), f"{no_skills / total * 100:.1f}%"
    )
    console.print(table)


def _print_skills_by_location(df: pd.DataFrame) -> None:
    """Print top 3 skills per unique search_location."""
    if "search_location" not in df.columns:
        return
    locations = df["search_location"].dropna().unique()
    if len(locations) <= 1:
        return
    col = (
        "all_skills_comprehensive"
        if "all_skills_comprehensive" in df.columns
        else "all_skills_flat"
    )

    table = make_table("Top skills by search location", expand=True)
    table.add_column("Search location", style="bold", overflow="fold")
    table.add_column("Top skills", overflow="fold")
    for loc in sorted(locations):
        subset = df[df["search_location"] == loc]
        counter: Counter = Counter()
        for skills_list in subset[col]:
            counter.update(skills_list)
        top = ", ".join(f"{s}({c})" for s, c in counter.most_common(3))
        table.add_row(str(loc), top)
    console.print(table)


def print_report(
    df: pd.DataFrame,
    taxonomy: dict[str, list[tuple[str, str]]],
    existing_candidate_canonicals: set[str],
    candidate_threshold: int,
) -> None:
    """Print a human-readable frequency analysis to stdout."""
    print_panel(
        "SKILLS ANALYSIS",
        [f"{len(df)} unique job postings"],
        style="cyan",
    )

    _print_quality_summary(df)
    _print_top_skills(df)
    _print_category_breakdown(df, taxonomy)
    _print_top_locations(df)
    _print_skills_by_location(df)
    _print_salary_hints(df)
    _print_llm_section(df, taxonomy, existing_candidate_canonicals, candidate_threshold)


def save_excel(
    df: pd.DataFrame,
    output_path: Path,
    taxonomy: dict[str, list[tuple[str, str]]],
) -> None:
    """Export the analysis DataFrame to Excel with one column per skill category."""
    internal_columns = [
        "skills_raw",
        "skills_by_category",
        "all_skills_flat",
        "all_skills_comprehensive",
        "skills_llm",
        "has_description",
    ]
    export_df = df.drop(columns=[col for col in internal_columns if col in df.columns])

    source_col = (
        "skills_by_category" if "skills_by_category" in df.columns else "skills_raw"
    )
    # Collect all categories across jobs (taxonomy order first, then extras from LLM)
    all_categories: list[str] = list(taxonomy.keys())
    if source_col == "skills_by_category":
        extra = {cat for row in df[source_col] for cat in row if cat not in taxonomy}
        all_categories.extend(sorted(extra))

    for category in all_categories:
        export_df[category] = df[source_col].apply(
            lambda raw, cat=category: ", ".join(raw.get(cat, []))
        )

    export_df.to_excel(output_path, index=False)
    print_success(f"Excel saved → {output_path.resolve()}")


# ── Entry point helpers ───────────────────────────────────────────────────────


def resolve_input_paths(args: Namespace, data_dir: Path) -> list[Path] | None:
    """Determine which JSON file(s) to analyze based on CLI arguments.

    Returns a list of Paths, or None if no files are found and execution should stop.
    """
    if args.file:
        return [Path(args.file)]

    if args.all:
        paths = sorted(data_dir.glob("jobs_*.json"))
        if not paths:
            print_info("No job files found in data/. Run scrape.py first.")
            return None
        return paths

    # Default: today's file, or the latest available
    today_file = data_dir / f"jobs_{date.today().isoformat()}.json"
    if today_file.exists():
        return [today_file]

    all_files = sorted(data_dir.glob("jobs_*.json"))
    if not all_files:
        print_info("No job files found. Run scrape.py first.")
        return None

    latest_file = all_files[-1]
    print_info(f"Today's file not found, using latest: {latest_file}")
    return [latest_file]


def build_llm_client(base_url: str, model: str, api_key: str):
    """Initialise and return an OpenAI-compatible client for 9router, or None on failure."""
    try:
        from openai import OpenAI

        client = OpenAI(base_url=base_url, api_key=api_key)
        print_info(f"LLM extraction enabled → {model} via 9router")
        return client
    except ImportError:
        print_info("openai package not installed. Run: pip install openai")
        return None


# ── Entry point ───────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze scraped LinkedIn jobs")
    parser.add_argument("--file", type=str, help="Specific JSON file to analyze")
    parser.add_argument(
        "--all", action="store_true", help="Merge all JSON files in data/"
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Also extract skills via LLM (free, uses 9router at localhost:20128)",
    )
    parser.add_argument(
        "--promote",
        nargs="?",
        const=2,
        type=int,
        default=None,
        metavar="N",
        help="Promote pending LLM candidates with jobs_count >= N (default 2) into skills",
    )
    parser.add_argument(
        "--candidates",
        action="store_true",
        help="Show pending skill candidates queue (no analysis)",
    )
    return parser


def _handle_mode_only_paths(args: Namespace, conn: sqlite3.Connection) -> bool:
    if args.candidates:
        print_candidates(conn)
        return True

    if args.promote is not None and not args.file and not args.all:
        apply_candidates(conn, max(args.promote, 1))
        return True

    return False


def _load_run_context(
    args: Namespace,
    conn: sqlite3.Connection,
    data_dir: Path,
) -> (
    tuple[list[Path], dict[str, list[tuple[str, str]]], list[dict], object | None]
    | None
):
    paths = resolve_input_paths(args, data_dir)
    if paths is None:
        if args.promote is not None:
            apply_candidates(conn, max(args.promote, 1))
        return None

    if args.promote is not None:
        apply_candidates(conn, max(args.promote, 1))

    skills = load_skills(conn)
    term_count = sum(len(terms) for terms in skills.values())
    print_info(
        f"Skills loaded: {term_count} terms (+ aliases) across {len(skills)} categories"
    )

    print_info(f"Loading from: {[str(p) for p in paths]}")
    jobs = load_jobs(paths)
    print_info(f"Loaded {len(jobs)} unique jobs.")
    if not jobs:
        return None

    llm_client = None
    if args.llm:
        llm_client = build_llm_client(
            NINEROUTER_BASE_URL,
            NINEROUTER_MODEL,
            NINEROUTER_API_KEY,
        )

    return paths, skills, jobs, llm_client


def main() -> None:
    """Parse CLI arguments and run the requested analysis / promotion workflow."""
    args = _build_parser().parse_args()
    data_dir = Path(OUTPUT_DIR)

    conn = open_db(data_dir)
    try:
        init_db(conn)
        if _handle_mode_only_paths(args, conn):
            return

        run_context = _load_run_context(args, conn, data_dir)
        if run_context is None:
            return

        paths, skills, jobs, llm_client = run_context
        df = analyze(jobs, skills, llm_client=llm_client, conn=conn)

        if args.llm and llm_client:
            promote_llm_to_candidates(conn, threshold=LLM_CANDIDATE_THRESHOLD)

        existing_candidate_terms = {
            row["term"] for row in conn.execute("SELECT term FROM skill_candidates")
        }
    finally:
        conn.close()

    print_report(
        df,
        skills,
        existing_candidate_terms,
        candidate_threshold=LLM_CANDIDATE_THRESHOLD,
    )
    output_stem = paths[0].stem if len(paths) == 1 else "jobs_all"
    save_excel(df, data_dir / f"{output_stem}_analysis.xlsx", skills)


if __name__ == "__main__":
    main()
