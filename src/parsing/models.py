"""Domain models for the Alteryx-to-MSSQL conversion agent.

All models are Pydantic v2 BaseModel subclasses. Value objects use frozen=True
so they can be safely shared and cached across pipeline stages.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict


class FieldSchema(BaseModel):
    """A single column as inferred from Alteryx RecordInfo metadata."""

    model_config = ConfigDict(frozen=True)

    name: str
    alteryx_type: str  # V_String, V_WString, Int32, Int64, Double, DateTime, …
    size: int | None = None
    source: str | None = None


class ToolNode(BaseModel):
    """One Alteryx tool node extracted from the workflow XML."""

    model_config = ConfigDict(frozen=True)

    tool_id: int
    plugin: str  # Full plugin string, e.g. "AlteryxBasePluginsGui.Filter.Filter"
    tool_type: str  # Normalised short key, e.g. "filter"
    config: dict[str, Any]  # Parsed <Configuration> XML subtree
    annotation: str
    position: tuple[int, int]
    output_schema: list[FieldSchema]  # From <MetaInfo>/<RecordInfo>; may be empty
    macro_path: str | None = None  # Set for macro nodes (EngineSettings Macro="…")
    source_type: str | None = None  # Set for input tools, e.g. "SQL Database", "Amazon S3"


class Connection(BaseModel):
    """A directed data-flow edge between two Alteryx tools."""

    model_config = ConfigDict(frozen=True)

    origin_id: int
    origin_anchor: str  # "Output", "True", "False", "Left", "Right", "Join", "Unique"
    dest_id: int
    dest_anchor: str  # "Input", "Input2", "Left", "Right"
    wireless: bool = False
    order: int | None = None  # From connection name="#1"/"#2" — controls union order


class ParsedWorkflow(BaseModel):
    """The raw output of the XML parser before DAG construction."""

    nodes: list[ToolNode]
    connections: list[Connection]
    source_file: str


class Chunk(BaseModel):
    """A translatable unit of the DAG: one or more tools converted together."""

    model_config = ConfigDict(frozen=True)

    chunk_id: int
    nodes: list[ToolNode]
    edges: list[Connection]
    input_cte_names: list[str]
    output_cte_name: str


class CTEFragment(BaseModel):
    """The SQL body for a single CTE block."""

    model_config = ConfigDict(frozen=True)

    name: str
    sql: str  # Body only — the part inside "name AS ( <sql> )"
    source_tool_ids: list[int]
    is_stub: bool = False
    # Set by the LLM repair pass when column-reference errors were auto-fixed.
    llm_repaired: bool = False
    llm_repair_notes: str = ""


class AlteryxStepDoc(BaseModel):
    """Documentation entry for one Alteryx tool, used in the workflow docs."""

    model_config = ConfigDict(frozen=True)

    tool_id: int
    tool_type: str
    annotation: str
    config_summary: str  # Human-readable one-liner of what the tool does


class SQLStepDoc(BaseModel):
    """Documentation entry for one generated CTE."""

    model_config = ConfigDict(frozen=True)

    cte_name: str
    sql_body: str
    source_tool_ids: list[int]
    is_stub: bool


class WorkflowDoc(BaseModel):
    """Full documentation for a converted workflow, rendered to Markdown."""

    workflow_summary: str
    alteryx_steps: list[AlteryxStepDoc]
    sql_steps: list[SQLStepDoc]
    notes: list[str]  # Warnings, caveats, manual review items


class RegistryEntry(BaseModel):
    """One learned tool translation stored in the persistent tool registry."""

    model_config = ConfigDict(frozen=True)

    plugin: str  # Full plugin string — registry key
    tool_type: str  # Normalised short name, e.g. "custom_aggregator"
    description: str  # One-line description of what the tool does
    sql_body: str  # CTE body SQL that was learned for this tool type
    learned_at: str  # ISO-8601 datetime string
    example_config_hash: str  # Short hash of the config used when learning


class ConversionResult(BaseModel):
    """The complete output of one workflow conversion run."""

    sql: str
    cte_fragments: list[CTEFragment]
    warnings: list[str]
    workflow_doc: WorkflowDoc
