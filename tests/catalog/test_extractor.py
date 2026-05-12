"""Tests for src/catalog/extractor.py.

Uses temporary directories with fixture SQL content — no LLM calls.
"""

import json

import pytest

from catalog.extractor import extract_catalog, load_catalog, save_catalog

_SP_A = """\
CREATE PROCEDURE [dbo].[SP_A] AS BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [Systime].[OutputA];
    WITH cte AS (
        SELECT [ISBN], [myAccountId]
        FROM [$(Datahub_Hist)].[Systime].[ACCOUNTING_CLEAN_V]
        WHERE [myAccountId] != 'X'
    )
    INSERT INTO [Systime].[OutputA]
    SELECT * FROM cte;
END;
GO
"""

_SP_B = """\
CREATE PROCEDURE [dbo].[SP_B] AS BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [Systime].[OutputB];
    SELECT [ISBN], COUNT(*) AS cnt
    FROM [$(Datahub_Hist)].[Systime].[ACCOUNTING_CLEAN_V]
    JOIN [dbo].[RefTable] ON [dbo].[RefTable].[id] = [ISBN]
    GROUP BY [ISBN]
    INSERT INTO [Systime].[OutputB]
    SELECT [ISBN], cnt FROM #tmp;
END;
GO
"""

_SP_C = """\
CREATE PROCEDURE [dbo].[SP_C] AS BEGIN
    SET NOCOUNT ON;
    SELECT * FROM [CooperC5].[dbo].[Publications];
END;
GO
"""


@pytest.fixture()
def output_dir(tmp_path):
    (tmp_path / "SP_A.sql").write_text(_SP_A, encoding="utf-8")
    (tmp_path / "SP_B.sql").write_text(_SP_B, encoding="utf-8")
    (tmp_path / "SP_C.sql").write_text(_SP_C, encoding="utf-8")
    return tmp_path


class TestExtractCatalog:
    def test_discovers_all_sp_files(self, output_dir):
        catalog = extract_catalog(output_dir)
        names = {e.workflow_name for e in catalog.workflows}
        assert names == {"SP_A", "SP_B", "SP_C"}

    def test_shared_accounting_table_in_table_usage(self, output_dir):
        catalog = extract_catalog(output_dir)
        # ACCOUNTING_CLEAN_V is read by both SP_A and SP_B
        key = "systime.accounting_clean_v"
        assert key in catalog.table_usage
        assert set(catalog.table_usage[key]) == {"SP_A", "SP_B"}

    def test_target_tables_extracted(self, output_dir):
        catalog = extract_catalog(output_dir)
        entry_a = next(e for e in catalog.workflows if e.workflow_name == "SP_A")
        target_keys = {r.catalog_key for r in entry_a.target_tables}
        assert "systime.outputa" in target_keys

    def test_single_use_table_not_shared(self, output_dir):
        catalog = extract_catalog(output_dir)
        # Publications is only read by SP_C
        pubs_key = "dbo.publications"
        if pubs_key in catalog.table_usage:
            assert len(catalog.table_usage[pubs_key]) == 1

    def test_temp_tables_excluded(self, output_dir):
        catalog = extract_catalog(output_dir)
        all_keys = set(catalog.table_usage.keys())
        for key in all_keys:
            assert "#" not in key

    def test_skip_enhanced_sql(self, output_dir):
        # Enhanced files should be ignored
        (output_dir / "SP_A_enhanced.sql").write_text(_SP_A, encoding="utf-8")
        catalog = extract_catalog(output_dir)
        names = {e.workflow_name for e in catalog.workflows}
        assert "SP_A_enhanced" not in names

    def test_skip_silver_gold_subdirs(self, output_dir):
        silver_dir = output_dir / "silver"
        silver_dir.mkdir()
        (silver_dir / "Load_AccountingClean.sql").write_text(_SP_A, encoding="utf-8")
        catalog = extract_catalog(output_dir)
        names = {e.workflow_name for e in catalog.workflows}
        assert "Load_AccountingClean" not in names


class TestSaveLoadCatalog:
    def test_round_trip(self, output_dir):
        catalog = extract_catalog(output_dir)
        save_catalog(catalog, output_dir)
        loaded = load_catalog(output_dir)
        assert loaded.model_dump() == catalog.model_dump()

    def test_catalog_json_written(self, output_dir):
        extract_catalog(output_dir)
        save_catalog(extract_catalog(output_dir), output_dir)
        assert (output_dir / "catalog.json").exists()
        data = json.loads((output_dir / "catalog.json").read_text())
        assert "workflows" in data
        assert "table_usage" in data

    def test_load_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="catalog.json"):
            load_catalog(tmp_path)
