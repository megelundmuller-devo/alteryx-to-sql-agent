"""AI agent that proposes silver tables; gold tables derived deterministically.

Silver tables: AI proposes based on shared source tables across SPs.
Gold tables: derived from the catalog's target_tables — each existing SP output
table becomes a gold SPROC that reads from silver tables instead of raw sources.

Writes two files:
    <output_dir>/medallion_plan.json  — structured plan for the generator
    <output_dir>/medallion_plan.md   — human-readable plan for review

Usage:
    from catalog.medallion_planner import plan_medallion
    plan = plan_medallion(catalog, output_dir)
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel
from pydantic_ai import Agent

from catalog.extractor import _parse_table_ref
from catalog.models import CrossWorkflowCatalog, GoldTablePlan, MedallionPlan, SilverTablePlan
from llm.settings import get_settings

_MIN_SHARED_WORKFLOWS = 2  # Only consider tables used by this many or more SPs
_SQL_SNIPPET_LINES = 60  # Lines to sample from each SP that reads a shared table


class _SilverPlan(BaseModel):
    """Intermediate AI output — silver tables only; gold built deterministically."""

    silver_tables: list[SilverTablePlan]
    notes: str


_SYSTEM_PROMPT = """\
You are a senior data engineer designing the silver layer of a medallion architecture
for Microsoft SQL Server.

You will receive:
1. A JSON catalog describing which stored procedures read and write which tables.
2. SQL snippets from the stored procedures that read shared source tables.

Your task is to propose silver layer tables only.

Silver tables:
- One silver table per shared source table that is meaningfully transformed (filtered,
  cleaned, enriched) by multiple workflows in a similar way.
- A silver table should cover the common transformations shared across workflows.
  Do NOT create silver tables for tables only ever read without transformation
  (pure pass-through SELECTs), or for tables used by only one workflow.
- Name format: [silver].[PascalCaseName], SPROC: [silver].[Load_PascalCaseName]

For suggested_columns: list T-SQL column names (without brackets) inferred from the
SQL snippets. Be conservative — only include columns you can see being selected.

Keep rationale concise (2-3 sentences). Be specific about which workflows benefit.

In the notes field: summarise the overall silver-layer approach and flag anything
reviewers should manually validate before running the generator.
"""


def _make_agent() -> Agent:
    settings = get_settings()
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)
    return Agent(
        settings.model_id,
        output_type=_SilverPlan,
        system_prompt=_SYSTEM_PROMPT,
        retries=settings.llm_max_retries,
    )


def _build_sql_snippets(
    catalog: CrossWorkflowCatalog,
    shared_keys: set[str],
) -> dict[str, list[tuple[str, str]]]:
    """Return {catalog_key: [(workflow_name, sql_snippet), ...]} for shared tables."""
    snippets: dict[str, list[tuple[str, str]]] = {k: [] for k in shared_keys}

    for entry in catalog.workflows:
        for ref in entry.source_tables:
            if ref.catalog_key not in shared_keys:
                continue
            sql_path = Path(entry.sql_path)
            if not sql_path.exists():
                continue
            lines = sql_path.read_text(encoding="utf-8").splitlines()
            target = ref.full_ref.lower()
            start = next(
                (i for i, line in enumerate(lines) if target in line.lower()),
                0,
            )
            snippet_start = max(0, start - 5)
            snippet_end = min(len(lines), snippet_start + _SQL_SNIPPET_LINES)
            snippet = "\n".join(lines[snippet_start:snippet_end])
            snippets[ref.catalog_key].append((entry.workflow_name, snippet))

    return snippets


def _build_prompt(
    catalog: CrossWorkflowCatalog,
    snippets: dict[str, list[tuple[str, str]]],
) -> str:
    parts: list[str] = []

    catalog_compact = {
        "workflows": [
            {
                "name": w.workflow_name,
                "source_tables": [r.full_ref for r in w.source_tables],
                "target_tables": [r.full_ref for r in w.target_tables],
            }
            for w in catalog.workflows
        ],
        "shared_source_tables": {
            k: v
            for k, v in catalog.table_usage.items()
            if len(v) >= _MIN_SHARED_WORKFLOWS
        },
    }
    parts.append("## Catalog\n")
    parts.append(f"```json\n{json.dumps(catalog_compact, indent=2)}\n```\n")

    if snippets:
        parts.append("\n## SQL Snippets for Shared Source Tables\n")
        for key, wf_snippets in snippets.items():
            if not wf_snippets:
                continue
            parts.append(f"\n### Source table: `{key}`\n")
            for wf_name, snippet in wf_snippets:
                parts.append(f"\n**From {wf_name}:**\n```sql\n{snippet}\n```\n")

    return "\n".join(parts)


def _build_gold_plans(
    catalog: CrossWorkflowCatalog,
    silver_tables: list[SilverTablePlan],
) -> list[GoldTablePlan]:
    """Derive gold table plans from existing SP target_tables + the silver plan.

    Each workflow that writes to a target table AND has at least one source that
    maps to a silver table becomes a gold SPROC.  The gold SPROC is the existing
    SP refactored to read from silver instead of the raw source.
    """
    # catalog_key → silver table_name
    silver_by_source: dict[str, str] = {}
    for s in silver_tables:
        try:
            ref = _parse_table_ref(s.source_table)
            silver_by_source[ref.catalog_key] = s.table_name
        except Exception:  # noqa: BLE001
            pass

    gold_plans: list[GoldTablePlan] = []
    for entry in catalog.workflows:
        if not entry.target_tables:
            continue

        # Map raw source full_refs → silver table names where a silver exists
        substitutions: dict[str, str] = {
            src.full_ref: silver_by_source[src.catalog_key]
            for src in entry.source_tables
            if src.catalog_key in silver_by_source
        }
        if not substitutions:
            continue  # No silver tables for any of this workflow's sources

        for tgt in entry.target_tables:
            tbl_name = f"[{tgt.schema_name}].[{tgt.table_name}]"
            sproc_name = f"[{tgt.schema_name}].[Load_{tgt.table_name}]"
            silver_names = list(dict.fromkeys(substitutions.values()))  # dedupe, preserve order
            gold_plans.append(
                GoldTablePlan(
                    table_name=tbl_name,
                    sproc_name=sproc_name,
                    source_workflow_name=entry.workflow_name,
                    source_silver_tables=silver_names,
                    silver_substitutions=substitutions,
                    rationale=(
                        f"Refactored from {entry.workflow_name}. "
                        f"Replaces raw source reads with silver layer equivalents."
                    ),
                    used_by_workflows=[entry.workflow_name],
                )
            )

    return gold_plans


def _render_plan_markdown(plan: MedallionPlan) -> str:
    """Render a MedallionPlan to a human-readable, editable Markdown file."""
    lines: list[str] = [
        "# Medallion Architecture Plan",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "\n> Review this file and edit as needed, then run "
        "`medallion generate` to produce the SPROC scripts.\n",
    ]

    lines.append("## Silver Tables\n")
    if plan.silver_tables:
        for tbl in plan.silver_tables:
            lines.append(f"### {tbl.table_name}\n")
            lines.append(f"- **SPROC:** `{tbl.sproc_name}`")
            lines.append(f"- **Source:** `{tbl.source_table}`")
            lines.append(f"- **Used by:** {', '.join(tbl.used_by_workflows)}")
            lines.append(f"- **Rationale:** {tbl.rationale}")
            if tbl.suggested_columns:
                col_list = ", ".join(f"`{c}`" for c in tbl.suggested_columns)
                lines.append(f"- **Suggested columns:** {col_list}")
            lines.append("")
    else:
        lines.append("_No silver tables proposed._\n")

    lines.append("## Gold Tables (refactored from existing SPs)\n")
    if plan.gold_tables:
        for tbl in plan.gold_tables:
            lines.append(f"### {tbl.table_name}\n")
            lines.append(f"- **SPROC:** `{tbl.sproc_name}`")
            lines.append(f"- **Derived from:** `{tbl.source_workflow_name}`")
            sources = ", ".join(f"`{s}`" for s in tbl.source_silver_tables)
            lines.append(f"- **Silver sources:** {sources}")
            lines.append("- **Substitutions:**")
            for raw, silver in tbl.silver_substitutions.items():
                lines.append(f"  - `{raw}` → `{silver}`")
            lines.append(f"- **Rationale:** {tbl.rationale}")
            lines.append("")
    else:
        lines.append("_No gold tables derived (no silver sources matched SP outputs)._\n")

    lines.append("## Notes\n")
    lines.append(plan.notes)

    return "\n".join(lines)


def plan_medallion(catalog: CrossWorkflowCatalog, output_dir: Path) -> MedallionPlan:
    """Propose silver tables (AI) and derive gold tables (deterministic).

    Returns the combined MedallionPlan. Also writes:
        <output_dir>/medallion_plan.json
        <output_dir>/medallion_plan.md
    """
    shared_keys = {
        k for k, wfs in catalog.table_usage.items() if len(wfs) >= _MIN_SHARED_WORKFLOWS
    }

    snippets = _build_sql_snippets(catalog, shared_keys)
    prompt = _build_prompt(catalog, snippets)

    agent = _make_agent()
    result = agent.run_sync(prompt)
    silver_plan: _SilverPlan = result.output

    gold_plans = _build_gold_plans(catalog, silver_plan.silver_tables)

    plan = MedallionPlan(
        silver_tables=silver_plan.silver_tables,
        gold_tables=gold_plans,
        notes=silver_plan.notes,
    )

    plan_json_path = output_dir / "medallion_plan.json"
    plan_json_path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")

    plan_md_path = output_dir / "medallion_plan.md"
    plan_md_path.write_text(_render_plan_markdown(plan), encoding="utf-8")

    return plan


def load_plan(output_dir: Path) -> MedallionPlan:
    """Load and validate medallion_plan.json from *output_dir*."""
    plan_path = output_dir / "medallion_plan.json"
    if not plan_path.exists():
        raise FileNotFoundError(
            f"medallion_plan.json not found in {output_dir} — run 'medallion plan' first."
        )
    data = json.loads(plan_path.read_text(encoding="utf-8"))
    return MedallionPlan.model_validate(data)
