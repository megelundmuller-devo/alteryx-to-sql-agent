"""Tests for src/catalog/medallion_planner.py.

All LLM calls are mocked — no real Vertex AI calls.
"""

from unittest.mock import MagicMock, patch

import pytest

from catalog.medallion_planner import (
    _build_gold_plans,
    _render_plan_markdown,
    load_plan,
    plan_medallion,
)
from catalog.models import (
    CrossWorkflowCatalog,
    MedallionPlan,
    SilverTablePlan,
    TableRef,
    WorkflowEntry,
)


def _make_ref(schema: str, table: str, db: str = "$(Datahub_Hist)") -> TableRef:
    full_ref = f"[{db}].{schema}.{table}"
    return TableRef(
        schema_name=schema,
        table_name=table,
        full_ref=full_ref,
        catalog_key=f"{schema}.{table}".lower(),
    )


def _make_target_ref(schema: str, table: str) -> TableRef:
    return TableRef(
        schema_name=schema,
        table_name=table,
        full_ref=f"[{schema}].[{table}]",
        catalog_key=f"{schema}.{table}".lower(),
    )


def _make_catalog() -> CrossWorkflowCatalog:
    ref = _make_ref("Systime", "ACCOUNTING_CLEAN_V")
    tgt = _make_target_ref("Systime", "LifeTimeUniqueUsers")
    return CrossWorkflowCatalog(
        workflows=[
            WorkflowEntry(
                workflow_name="SP_A",
                sql_path="/fake/SP_A.sql",
                docs_path="",
                source_tables=[ref],
                target_tables=[tgt],
            ),
            WorkflowEntry(
                workflow_name="SP_B",
                sql_path="/fake/SP_B.sql",
                docs_path="",
                source_tables=[ref],
                target_tables=[],
            ),
        ],
        table_usage={"systime.accounting_clean_v": ["SP_A", "SP_B"]},
    )


def _make_silver_plan() -> list[SilverTablePlan]:
    return [
        SilverTablePlan(
            table_name="[silver].[AccountingClean]",
            sproc_name="[silver].[Load_AccountingClean]",
            source_table="[$(Datahub_Hist)].Systime.ACCOUNTING_CLEAN_V",
            rationale="Shared by SP_A and SP_B.",
            used_by_workflows=["SP_A", "SP_B"],
            suggested_columns=["ISBN", "myAccountId"],
        )
    ]


def _make_plan(gold: bool = True) -> MedallionPlan:
    from catalog.models import GoldTablePlan

    silver = _make_silver_plan()
    gold_tables = (
        [
            GoldTablePlan(
                table_name="[Systime].[LifeTimeUniqueUsers]",
                sproc_name="[Systime].[Load_LifeTimeUniqueUsers]",
                source_workflow_name="SP_A",
                source_silver_tables=["[silver].[AccountingClean]"],
                silver_substitutions={
                    "[$(Datahub_Hist)].Systime.ACCOUNTING_CLEAN_V": "[silver].[AccountingClean]"
                },
                rationale="Refactored from SP_A.",
                used_by_workflows=["SP_A"],
            )
        ]
        if gold
        else []
    )
    return MedallionPlan(silver_tables=silver, gold_tables=gold_tables, notes="Review first.")


class TestBuildGoldPlans:
    def test_derives_gold_from_target_tables(self):
        catalog = _make_catalog()
        silver = _make_silver_plan()
        gold = _build_gold_plans(catalog, silver)
        assert len(gold) == 1
        assert gold[0].table_name == "[Systime].[LifeTimeUniqueUsers]"
        assert gold[0].source_workflow_name == "SP_A"

    def test_substitution_map_populated(self):
        catalog = _make_catalog()
        silver = _make_silver_plan()
        gold = _build_gold_plans(catalog, silver)
        subs = gold[0].silver_substitutions
        # The raw full_ref should map to the silver table name
        assert any("[silver].[AccountingClean]" in v for v in subs.values())

    def test_workflow_without_target_skipped(self):
        catalog = _make_catalog()
        silver = _make_silver_plan()
        gold = _build_gold_plans(catalog, silver)
        names = [g.source_workflow_name for g in gold]
        assert "SP_B" not in names  # SP_B has no target tables

    def test_no_silver_match_skipped(self):
        ref = _make_ref("dbo", "SomeOtherTable")
        tgt = _make_target_ref("dbo", "OutputTable")
        catalog = CrossWorkflowCatalog(
            workflows=[
                WorkflowEntry(
                    workflow_name="SP_X",
                    sql_path="/fake/SP_X.sql",
                    docs_path="",
                    source_tables=[ref],
                    target_tables=[tgt],
                )
            ],
            table_usage={"dbo.someothertable": ["SP_X"]},
        )
        silver = _make_silver_plan()  # covers accounting only, not SomeOtherTable
        gold = _build_gold_plans(catalog, silver)
        assert gold == []


class TestRenderPlanMarkdown:
    def test_contains_silver_table_name(self):
        md = _render_plan_markdown(_make_plan())
        assert "[silver].[AccountingClean]" in md

    def test_contains_gold_table_name(self):
        md = _render_plan_markdown(_make_plan())
        assert "[Systime].[LifeTimeUniqueUsers]" in md

    def test_contains_substitution(self):
        md = _render_plan_markdown(_make_plan())
        assert "[silver].[AccountingClean]" in md

    def test_no_gold_shows_placeholder(self):
        md = _render_plan_markdown(_make_plan(gold=False))
        assert "No gold tables" in md

    def test_derived_from_shown(self):
        md = _render_plan_markdown(_make_plan())
        assert "SP_A" in md


class TestPlanMedallion:
    def test_writes_json_and_md(self, tmp_path):
        catalog = _make_catalog()
        from catalog.medallion_planner import _SilverPlan

        silver_output = _SilverPlan(
            silver_tables=_make_silver_plan(), notes="OK"
        )
        mock_result = MagicMock()
        mock_result.output = silver_output
        mock_agent = MagicMock()
        mock_agent.run_sync.return_value = mock_result

        with patch("catalog.medallion_planner._make_agent", return_value=mock_agent):
            plan = plan_medallion(catalog, tmp_path)

        assert (tmp_path / "medallion_plan.json").exists()
        assert (tmp_path / "medallion_plan.md").exists()
        assert len(plan.silver_tables) == 1
        assert len(plan.gold_tables) == 1  # derived from SP_A's target table

    def test_gold_derived_not_invented(self, tmp_path):
        catalog = _make_catalog()
        from catalog.medallion_planner import _SilverPlan

        silver_output = _SilverPlan(silver_tables=_make_silver_plan(), notes="OK")
        mock_result = MagicMock()
        mock_result.output = silver_output
        mock_agent = MagicMock()
        mock_agent.run_sync.return_value = mock_result

        with patch("catalog.medallion_planner._make_agent", return_value=mock_agent):
            plan = plan_medallion(catalog, tmp_path)

        assert plan.gold_tables[0].source_workflow_name == "SP_A"
        assert plan.gold_tables[0].table_name == "[Systime].[LifeTimeUniqueUsers]"


class TestLoadPlan:
    def test_load_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="medallion_plan.json"):
            load_plan(tmp_path)

    def test_round_trip(self, tmp_path):
        plan = _make_plan()
        (tmp_path / "medallion_plan.json").write_text(plan.model_dump_json(), encoding="utf-8")
        loaded = load_plan(tmp_path)
        assert loaded.gold_tables[0].source_workflow_name == "SP_A"
