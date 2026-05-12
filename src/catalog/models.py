"""Pydantic models for the cross-workflow catalog and medallion architecture plan.

The catalog captures table-level lineage across all generated stored procedures.
The medallion plan describes which silver and gold layer SPROCs to create.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class TableRef(BaseModel):
    """A normalised reference to a database table."""

    model_config = ConfigDict(frozen=True)

    schema_name: str  # e.g. "Systime" (without brackets)
    table_name: str  # e.g. "ACCOUNTING_CLEAN_V" (without brackets)
    full_ref: str  # e.g. "[$(Datahub_Hist)].[Systime].[ACCOUNTING_CLEAN_V]"
    catalog_key: str  # Lowercase "schema.table" used for cross-workflow dedup


class WorkflowEntry(BaseModel):
    """Catalog entry for one generated stored procedure."""

    model_config = ConfigDict(frozen=True)

    workflow_name: str  # Stem of the .sql file, e.g. "SP_LifeTimeUniqueUsers"
    sql_path: str  # Absolute path to the .sql file
    docs_path: str  # Absolute path to the _docs.md file (empty string if absent)
    source_tables: list[TableRef]  # Tables this SP reads from
    target_tables: list[TableRef]  # Tables this SP writes to (TRUNCATE / INSERT INTO)


class CrossWorkflowCatalog(BaseModel):
    """Aggregated table-lineage catalog across all generated SPs in an output directory."""

    workflows: list[WorkflowEntry]
    # catalog_key → list of workflow_names that read this table
    table_usage: dict[str, list[str]]


class SilverTablePlan(BaseModel):
    """Plan for one silver-layer table and its batch-load SPROC."""

    model_config = ConfigDict(frozen=True)

    table_name: str  # e.g. "[silver].[AccountingClean]"
    sproc_name: str  # e.g. "[silver].[Load_AccountingClean]"
    source_table: str  # The raw source table this silver table reads from
    rationale: str
    used_by_workflows: list[str]
    suggested_columns: list[str]  # Column names inferred from source SQL samples


class GoldTablePlan(BaseModel):
    """Plan for one gold-layer SPROC — a refactored version of an existing SP.

    Gold tables are the existing SP output tables.  The gold SPROC is derived
    from the original SP by substituting raw source table references with the
    corresponding silver table references.
    """

    model_config = ConfigDict(frozen=True)

    table_name: str  # Existing output table, e.g. "[Systime].[LifeTimeUniqueUsers]"
    sproc_name: str  # New SPROC name, e.g. "[Systime].[Load_LifeTimeUniqueUsers]"
    source_workflow_name: str  # The SP file stem this gold SPROC is derived from
    source_silver_tables: list[str]  # Silver table names used as sources
    # Mapping: raw full_ref → silver table_name (used by the generator for substitution)
    silver_substitutions: dict[str, str]
    rationale: str
    used_by_workflows: list[str]


class MedallionPlan(BaseModel):
    """AI-proposed medallion architecture across all workflows in an output directory."""

    silver_tables: list[SilverTablePlan]
    gold_tables: list[GoldTablePlan]
    notes: str  # Overall rationale, caveats, or instructions for reviewers
