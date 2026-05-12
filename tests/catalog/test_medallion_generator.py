"""Tests for src/catalog/medallion_generator.py.

All LLM calls are mocked — no real Vertex AI calls.
"""

from unittest.mock import MagicMock, patch

from catalog.medallion_generator import _build_gold_prompt, _sproc_filename, generate_medallion
from catalog.models import (
    CrossWorkflowCatalog,
    GoldTablePlan,
    MedallionPlan,
    SilverTablePlan,
    TableRef,
    WorkflowEntry,
)

_EXISTING_SP_SQL = """\
CREATE PROCEDURE [Systime].[SP_A] AS BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [Systime].[LifeTimeUniqueUsers];
    SELECT [ISBN] FROM [$(Hub)].Systime.ACCOUNTING_CLEAN_V;
END;
GO
"""

_FAKE_SILVER_SPROC = """\
CREATE OR ALTER PROCEDURE [silver].[Load_AccountingClean]
AS
BEGIN
    SET NOCOUNT ON;
    IF OBJECT_ID('[silver].[AccountingClean]', 'U') IS NULL
    CREATE TABLE [silver].[AccountingClean] ([ISBN] NVARCHAR(50));
    TRUNCATE TABLE [silver].[AccountingClean];
    INSERT INTO [silver].[AccountingClean] ([ISBN])
    SELECT [ISBN] FROM [$(Hub)].Systime.ACCOUNTING_CLEAN_V;
END;
GO
"""

_FAKE_GOLD_SPROC = """\
CREATE OR ALTER PROCEDURE [Systime].[Load_LifeTimeUniqueUsers]
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [Systime].[LifeTimeUniqueUsers];
    SELECT [ISBN] FROM [silver].[AccountingClean];
END;
GO
"""


def _make_catalog(tmp_path) -> CrossWorkflowCatalog:
    sp_path = tmp_path / "SP_A.sql"
    sp_path.write_text(_EXISTING_SP_SQL, encoding="utf-8")

    ref = TableRef(
        schema_name="Systime",
        table_name="ACCOUNTING_CLEAN_V",
        full_ref="[$(Hub)].Systime.ACCOUNTING_CLEAN_V",
        catalog_key="systime.accounting_clean_v",
    )
    tgt = TableRef(
        schema_name="Systime",
        table_name="LifeTimeUniqueUsers",
        full_ref="[Systime].[LifeTimeUniqueUsers]",
        catalog_key="systime.lifetimeuniqueusers",
    )
    return CrossWorkflowCatalog(
        workflows=[
            WorkflowEntry(
                workflow_name="SP_A",
                sql_path=str(sp_path),
                docs_path="",
                source_tables=[ref],
                target_tables=[tgt],
            )
        ],
        table_usage={"systime.accounting_clean_v": ["SP_A"]},
    )


def _make_plan() -> MedallionPlan:
    return MedallionPlan(
        silver_tables=[
            SilverTablePlan(
                table_name="[silver].[AccountingClean]",
                sproc_name="[silver].[Load_AccountingClean]",
                source_table="[$(Hub)].Systime.ACCOUNTING_CLEAN_V",
                rationale="Shared source.",
                used_by_workflows=["SP_A"],
                suggested_columns=["ISBN"],
            )
        ],
        gold_tables=[
            GoldTablePlan(
                table_name="[Systime].[LifeTimeUniqueUsers]",
                sproc_name="[Systime].[Load_LifeTimeUniqueUsers]",
                source_workflow_name="SP_A",
                source_silver_tables=["[silver].[AccountingClean]"],
                silver_substitutions={
                    "[$(Hub)].Systime.ACCOUNTING_CLEAN_V": "[silver].[AccountingClean]"
                },
                rationale="Refactored from SP_A.",
                used_by_workflows=["SP_A"],
            )
        ],
        notes="Test plan.",
    )


class TestSprocFilename:
    def test_two_part_ref(self):
        assert _sproc_filename("[silver].[Load_AccountingClean]") == "Load_AccountingClean.sql"

    def test_single_segment(self):
        assert _sproc_filename("[Load_AccountingClean]") == "Load_AccountingClean.sql"


class TestBuildGoldPrompt:
    def test_contains_substitution_map(self):
        plan = _make_plan()
        prompt = _build_gold_prompt(plan.gold_tables[0], _EXISTING_SP_SQL)
        assert "[$(Hub)].Systime.ACCOUNTING_CLEAN_V" in prompt
        assert "[silver].[AccountingClean]" in prompt

    def test_contains_existing_sql(self):
        plan = _make_plan()
        prompt = _build_gold_prompt(plan.gold_tables[0], _EXISTING_SP_SQL)
        assert "SP_A" in prompt
        assert "TRUNCATE TABLE" in prompt


class TestGenerateMedallion:
    def _make_agents(self, silver_sql: str, gold_sql: str):
        silver_result = MagicMock()
        silver_result.output = silver_sql
        silver_agent = MagicMock()
        silver_agent.run_sync.return_value = silver_result

        gold_result = MagicMock()
        gold_result.output = gold_sql
        gold_agent = MagicMock()
        gold_agent.run_sync.return_value = gold_result

        return silver_agent, gold_agent

    def test_silver_and_gold_files_written(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        plan = _make_plan()
        silver_agent, gold_agent = self._make_agents(_FAKE_SILVER_SPROC, _FAKE_GOLD_SPROC)

        with (
            patch("catalog.medallion_generator._make_silver_agent", return_value=silver_agent),
            patch("catalog.medallion_generator._make_gold_agent", return_value=gold_agent),
        ):
            written = generate_medallion(plan, catalog, tmp_path)

        silver_path = tmp_path / "silver" / "Load_AccountingClean.sql"
        gold_path = tmp_path / "gold" / "Load_LifeTimeUniqueUsers.sql"
        assert silver_path in written
        assert gold_path in written
        assert silver_path.exists()
        assert gold_path.exists()

    def test_gold_prompt_uses_existing_sp_sql(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        plan = _make_plan()
        silver_agent, gold_agent = self._make_agents(_FAKE_SILVER_SPROC, _FAKE_GOLD_SPROC)

        with (
            patch("catalog.medallion_generator._make_silver_agent", return_value=silver_agent),
            patch("catalog.medallion_generator._make_gold_agent", return_value=gold_agent),
        ):
            generate_medallion(plan, catalog, tmp_path)

        # Gold agent prompt should have contained the existing SP SQL
        call_args = gold_agent.run_sync.call_args[0][0]
        assert "TRUNCATE TABLE [Systime].[LifeTimeUniqueUsers]" in call_args

    def test_markdown_fences_stripped(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        plan = _make_plan()
        silver_agent, gold_agent = self._make_agents(
            f"```sql\n{_FAKE_SILVER_SPROC}\n```", _FAKE_GOLD_SPROC
        )

        with (
            patch("catalog.medallion_generator._make_silver_agent", return_value=silver_agent),
            patch("catalog.medallion_generator._make_gold_agent", return_value=gold_agent),
        ):
            generate_medallion(plan, catalog, tmp_path)

        content = (tmp_path / "silver" / "Load_AccountingClean.sql").read_text()
        assert not content.startswith("```")

    def test_dirs_created(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        plan = _make_plan()
        silver_agent, gold_agent = self._make_agents(_FAKE_SILVER_SPROC, _FAKE_GOLD_SPROC)

        with (
            patch("catalog.medallion_generator._make_silver_agent", return_value=silver_agent),
            patch("catalog.medallion_generator._make_gold_agent", return_value=gold_agent),
        ):
            generate_medallion(plan, catalog, tmp_path)

        assert (tmp_path / "silver").is_dir()
        assert (tmp_path / "gold").is_dir()
