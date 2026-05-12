"""Generate silver and gold layer batch-load SPROCs from a MedallionPlan.

Silver SPROCs: new scripts that extract shared transformation logic from raw sources.
Gold SPROCs:  refactored versions of existing SPs that read from silver instead of raw.

Output is written to:
    <output_dir>/silver/Load_<TableName>.sql
    <output_dir>/gold/Load_<TableName>.sql

Usage:
    from catalog.medallion_generator import generate_medallion
    generate_medallion(plan, catalog, output_dir)
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic_ai import Agent

from catalog.models import CrossWorkflowCatalog, GoldTablePlan, MedallionPlan, SilverTablePlan
from llm.settings import get_settings

_SQL_CONTEXT_LINES = 80

_SILVER_SYSTEM_PROMPT = """\
You are a senior T-SQL developer writing batch-load stored procedures for a medallion
architecture on Microsoft SQL Server.

You will receive a silver table plan and SQL snippets from existing stored procedures
that read the same source table. Your job is to write a single CREATE OR ALTER PROCEDURE
script that:

1. Creates the silver target table if it does not already exist, with appropriate column
   definitions inferred from the SQL snippets and the suggested column list.
2. TRUNCATEs the silver target table.
3. INSERTs into the silver target table by SELECTing from the raw source table and
   applying the common transformations visible in the SQL snippets.

Rules:
- Use T-SQL / Microsoft SQL Server syntax exclusively.
- Quote all identifiers with square brackets: [schema].[table], [column].
- Use NVARCHAR for strings, DATETIME2 for dates, BIT for booleans, INT/BIGINT for ints.
- Use ISNULL() not COALESCE() where a single fallback suffices.
- Use TRY_CAST / TRY_CONVERT for safe type conversions.
- The procedure must follow this structure exactly:

    CREATE OR ALTER PROCEDURE [schema].[Load_TableName]
    AS
    BEGIN
        SET NOCOUNT ON;

        IF OBJECT_ID('[schema].[TableName]', 'U') IS NULL
        CREATE TABLE [schema].[TableName] (
            [Col1] NVARCHAR(255),
            ...
        );

        TRUNCATE TABLE [schema].[TableName];

        INSERT INTO [schema].[TableName] ([Col1], ...)
        SELECT
            ...
        FROM [raw_source_schema].[SourceTable]
        WHERE ...;
    END;
    GO

- Include a header comment block with the table name, source, and the list of
  workflows this replaces logic from.
- Output ONLY the complete T-SQL stored procedure — no explanation, no markdown fences.
"""

_GOLD_SYSTEM_PROMPT = """\
You are a senior T-SQL developer refactoring existing stored procedures to read from
a silver layer instead of raw source tables in a medallion architecture on Microsoft
SQL Server.

You will receive:
1. The existing stored procedure SQL.
2. A substitution map: raw source table → silver table replacement.

Your job is to produce a CREATE OR ALTER PROCEDURE script that is identical to the
original except:
- Every FROM / JOIN reference to a raw source table is replaced with the corresponding
  silver table reference from the substitution map.
- The SPROC name is updated to the specified new name.
- All other logic, CTEs, column names, WHERE clauses, JOINs, and output destinations
  are preserved exactly.

Rules:
- Use T-SQL / Microsoft SQL Server syntax exclusively.
- Quote all identifiers with square brackets: [schema].[table], [column].
- Do NOT simplify, refactor, or alter any logic beyond the table substitutions.
- Output ONLY the complete T-SQL stored procedure — no explanation, no markdown fences.
"""


def _make_silver_agent() -> Agent:
    settings = get_settings()
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)
    return Agent(
        settings.model_id,
        output_type=str,
        system_prompt=_SILVER_SYSTEM_PROMPT,
        retries=settings.llm_max_retries,
    )


def _make_gold_agent() -> Agent:
    settings = get_settings()
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)
    return Agent(
        settings.model_id,
        output_type=str,
        system_prompt=_GOLD_SYSTEM_PROMPT,
        retries=settings.llm_max_retries,
    )


def _strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text[text.index("\n") + 1 :] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[: text.rindex("```")].rstrip()
    return text.strip()


def _collect_silver_snippets(
    workflow_names: list[str],
    source_ref: str,
    catalog: CrossWorkflowCatalog,
) -> list[tuple[str, str]]:
    """Return [(workflow_name, sql_snippet)] for each workflow that reads source_ref."""
    source_lower = source_ref.lower()
    snippets: list[tuple[str, str]] = []

    entry_map = {e.workflow_name: e for e in catalog.workflows}
    for wf_name in workflow_names:
        entry = entry_map.get(wf_name)
        if entry is None:
            continue
        sql_path = Path(entry.sql_path)
        if not sql_path.exists():
            continue
        lines = sql_path.read_text(encoding="utf-8").splitlines()
        start = next(
            (i for i, line in enumerate(lines) if source_lower in line.lower()),
            0,
        )
        snippet_start = max(0, start - 5)
        snippet_end = min(len(lines), snippet_start + _SQL_CONTEXT_LINES)
        snippet = "\n".join(lines[snippet_start:snippet_end])
        snippets.append((wf_name, snippet))

    return snippets


def _build_silver_prompt(tbl: SilverTablePlan, snippets: list[tuple[str, str]]) -> str:
    parts: list[str] = [
        "## Silver Table Plan\n",
        f"- Table name: `{tbl.table_name}`",
        f"- SPROC name: `{tbl.sproc_name}`",
        f"- Source table: `{tbl.source_table}`",
        f"- Used by workflows: {', '.join(tbl.used_by_workflows)}",
        f"- Rationale: {tbl.rationale}",
    ]
    if tbl.suggested_columns:
        parts.append(f"- Suggested columns: {', '.join(tbl.suggested_columns)}")

    if snippets:
        parts.append("\n## SQL Snippets from Source Workflows\n")
        for wf_name, snippet in snippets:
            parts.append(f"### From {wf_name}:\n```sql\n{snippet}\n```\n")

    return "\n".join(parts)


def _build_gold_prompt(tbl: GoldTablePlan, existing_sql: str) -> str:
    parts: list[str] = [
        "## Gold SPROC Refactoring\n",
        f"- New SPROC name: `{tbl.sproc_name}`",
        f"- Output table (unchanged): `{tbl.table_name}`",
        f"- Source workflow: `{tbl.source_workflow_name}`",
        "\n## Source Table Substitutions\n",
        "Replace each raw table reference with the corresponding silver table:\n",
    ]
    for raw, silver in tbl.silver_substitutions.items():
        parts.append(f"- `{raw}` → `{silver}`")

    parts.append(f"\n## Existing SP SQL\n```sql\n{existing_sql}\n```")
    return "\n".join(parts)


def _sproc_filename(sproc_name: str) -> str:
    """Convert '[silver].[Load_AccountingClean]' to 'Load_AccountingClean.sql'."""
    parts = sproc_name.replace("[", "").replace("]", "").split(".")
    return f"{parts[-1]}.sql"


def generate_medallion(
    plan: MedallionPlan,
    catalog: CrossWorkflowCatalog,
    output_dir: Path,
) -> list[Path]:
    """Generate silver and gold SPROC scripts from *plan*.

    Returns the list of written file paths.
    """
    silver_agent = _make_silver_agent()
    gold_agent = _make_gold_agent()
    written: list[Path] = []

    silver_dir = output_dir / "silver"
    gold_dir = output_dir / "gold"
    silver_dir.mkdir(exist_ok=True)
    gold_dir.mkdir(exist_ok=True)

    for tbl in plan.silver_tables:
        snippets = _collect_silver_snippets(tbl.used_by_workflows, tbl.source_table, catalog)
        prompt = _build_silver_prompt(tbl, snippets)
        result = silver_agent.run_sync(prompt)
        sql = _strip_fences(result.output)

        out_path = silver_dir / _sproc_filename(tbl.sproc_name)
        out_path.write_text(sql, encoding="utf-8")
        written.append(out_path)

    entry_map = {e.workflow_name: e for e in catalog.workflows}
    for tbl in plan.gold_tables:
        entry = entry_map.get(tbl.source_workflow_name)
        existing_sql = ""
        if entry:
            sql_path = Path(entry.sql_path)
            if sql_path.exists():
                existing_sql = sql_path.read_text(encoding="utf-8")

        prompt = _build_gold_prompt(tbl, existing_sql)
        result = gold_agent.run_sync(prompt)
        sql = _strip_fences(result.output)

        out_path = gold_dir / _sproc_filename(tbl.sproc_name)
        out_path.write_text(sql, encoding="utf-8")
        written.append(out_path)

    return written
