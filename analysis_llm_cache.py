import hashlib
import sqlite3


def _url_key(url: str) -> str:
    """Return an MD5 hex digest of the URL for use as a DB cache key."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def _llm_cache_get(
    conn: sqlite3.Connection, url_key: str
) -> dict[str, list[str]] | None:
    """Return cached LLM extraction results for url_key, or None if not cached.

    Returns {category: [skills]} with a special '_matched' key for is_matched=1 rows
    to maintain internal API compatibility with _build_comprehensive_by_category.
    """
    rows = conn.execute(
        """SELECT c.name AS category, r.skill, r.is_matched
           FROM llm_results r
           JOIN categories c ON r.category_id = c.id
           WHERE r.url_key = ?""",
        (url_key,),
    ).fetchall()
    if not rows:
        return None
    cached_skills: dict[str, list[str]] = {}
    for row in rows:
        if row["is_matched"]:
            cached_skills.setdefault("_matched", []).append(row["skill"])
        else:
            cached_skills.setdefault(row["category"], []).append(row["skill"])
    return cached_skills


def _llm_cache_set(
    conn: sqlite3.Connection,
    url: str,
    url_key: str,
    result: dict[str, list[str]],
) -> None:
    """Persist LLM extraction results to the DB cache.

    Entries under '_matched' key are stored with is_matched=1 and their actual
    skills-catalog category looked up from the skills table.
    """
    for category, skills in result.items():
        is_matched = 1 if category == "_matched" else 0
        for skill in skills:
            if is_matched:
                # Look up category_id from the skills table via join
                row = conn.execute(
                    """SELECT s.category_id FROM skills s WHERE s.term = ?""",
                    (skill.lower(),),
                ).fetchone()
                if row is None:
                    continue
                cat_id = row["category_id"]
            else:
                row = conn.execute(
                    "SELECT id FROM categories WHERE name = ?", (category,)
                ).fetchone()
                if row is None:
                    continue
                cat_id = row["id"]
            conn.execute(
                "INSERT OR IGNORE INTO llm_results"
                "(url_key, url, category_id, skill, is_matched) VALUES(?,?,?,?,?)",
                (url_key, url, cat_id, skill, is_matched),
            )
    conn.commit()
