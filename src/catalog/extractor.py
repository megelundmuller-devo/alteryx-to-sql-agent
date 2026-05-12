"""Build a cross-workflow catalog by scanning generated .sql files.

Parses each stored-procedure SQL in an output directory using regex to extract
source tables (FROM / JOIN) and target tables (TRUNCATE TABLE / INSERT INTO).
Writes the result to catalog.json.

Usage:
    from catalog.extractor import extract_catalog, save_catalog
    catalog = extract_catalog(Path("output/"))
    save_catalog(catalog, Path("output/"))
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from catalog.models import CrossWorkflowCatalog, TableRef, WorkflowEntry

# One SQL identifier segment — either bracket-quoted [anything] or a plain word.
# Bracket form handles SQLCMD variables like [$(Datahub_Hist)].
_ID = r"(?:\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)"

# Two-part or three-part dotted ref: any mix of bracket/plain segments.
# Examples matched:
#   [$(Datahub_Hist)].Systime.ACCOUNTING_CLEAN_V   ← real output: bracket + 2 plain
#   [$(Datahub_Hist)].[Systime].[ACCOUNTING_CLEAN_V] ← fully bracketed
#   Systime.Salescube_Systime                        ← fully plain
_DOTTED_ID = rf"{_ID}\.{_ID}(?:\.{_ID})?"

# Keywords that introduce a source table reference
_FROM_RE = re.compile(
    rf"(?:FROM|JOIN)\s+({_DOTTED_ID})",
    re.IGNORECASE,
)

# Keywords that introduce a target table reference
_TARGET_RE = re.compile(
    rf"(?:TRUNCATE\s+TABLE|INSERT\s+INTO)\s+({_DOTTED_ID})",
    re.IGNORECASE,
)


def _strip_brackets(s: str) -> str:
    return s.strip("[]")


def _parse_table_ref(raw: str) -> TableRef:
    """Convert a raw bracket-quoted SQL ref into a TableRef."""
    segments = [_strip_brackets(p) for p in raw.split(".")]
    if len(segments) >= 2:
        schema_name = segments[-2]
        table_name = segments[-1]
    else:
        schema_name = "dbo"
        table_name = segments[0]

    catalog_key = f"{schema_name}.{table_name}".lower()
    return TableRef(
        schema_name=schema_name,
        table_name=table_name,
        full_ref=raw,
        catalog_key=catalog_key,
    )


def _is_temp_or_cte(ref: str) -> bool:
    """Return True for temp tables — identified by '#' anywhere in the ref."""
    return "#" in ref


def _extract_tables_from_sql(sql: str) -> tuple[list[TableRef], list[TableRef]]:
    """Return (source_tables, target_tables) parsed from a T-SQL stored procedure."""
    source_refs: dict[str, TableRef] = {}
    target_refs: dict[str, TableRef] = {}

    for match in _FROM_RE.finditer(sql):
        raw = match.group(1)
        if not _is_temp_or_cte(raw):
            ref = _parse_table_ref(raw)
            source_refs.setdefault(ref.catalog_key, ref)

    for match in _TARGET_RE.finditer(sql):
        raw = match.group(1)
        if not _is_temp_or_cte(raw):
            ref = _parse_table_ref(raw)
            target_refs.setdefault(ref.catalog_key, ref)

    return list(source_refs.values()), list(target_refs.values())


def _should_skip(sql_path: Path) -> bool:
    """Skip enhanced, silver, and gold layer scripts."""
    name = sql_path.name
    if name.endswith("_enhanced.sql"):
        return True
    parts = sql_path.parts
    for part in parts:
        if part in {"silver", "gold"}:
            return True
    return False


def extract_catalog(output_dir: Path) -> CrossWorkflowCatalog:
    """Scan *output_dir* for generated .sql files and build a CrossWorkflowCatalog.

    Only top-level .sql files are scanned (not silver/ or gold/ subdirectories,
    not *_enhanced.sql files).
    """
    sql_files = sorted(output_dir.glob("*.sql"))
    sql_files = [f for f in sql_files if not _should_skip(f)]

    workflows: list[WorkflowEntry] = []
    table_usage: dict[str, list[str]] = {}

    for sql_path in sql_files:
        sql_text = sql_path.read_text(encoding="utf-8")
        source_tables, target_tables = _extract_tables_from_sql(sql_text)

        docs_path = output_dir / f"{sql_path.stem}_docs.md"
        entry = WorkflowEntry(
            workflow_name=sql_path.stem,
            sql_path=str(sql_path),
            docs_path=str(docs_path) if docs_path.exists() else "",
            source_tables=source_tables,
            target_tables=target_tables,
        )
        workflows.append(entry)

        for ref in source_tables:
            table_usage.setdefault(ref.catalog_key, []).append(sql_path.stem)

    return CrossWorkflowCatalog(workflows=workflows, table_usage=table_usage)


def save_catalog(catalog: CrossWorkflowCatalog, output_dir: Path) -> Path:
    """Serialise *catalog* to <output_dir>/catalog.json and return the path."""
    catalog_path = output_dir / "catalog.json"
    catalog_path.write_text(
        catalog.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return catalog_path


def load_catalog(output_dir: Path) -> CrossWorkflowCatalog:
    """Load catalog.json from *output_dir*."""
    catalog_path = output_dir / "catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(
            f"catalog.json not found in {output_dir} — run 'medallion catalog' first."
        )
    data = json.loads(catalog_path.read_text(encoding="utf-8"))
    return CrossWorkflowCatalog.model_validate(data)
