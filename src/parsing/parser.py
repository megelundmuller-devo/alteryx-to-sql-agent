"""Parses Alteryx .yxmd XML files into ToolNode and Connection models.

Entry point: parse_workflow(path) -> ParsedWorkflow

The parser handles:
- All standard Alteryx tool types via a plugin-to-tool_type registry
- Macro nodes (EngineSettings Macro="…" with no Plugin attribute)
- ToolContainer nodes with nested ChildNodes (flattened into the output)
- RecordInfo schema extraction from MetaInfo elements
- Connection ordering (name="#1" / "#2") and wireless flags
"""

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from parsing.models import Connection, FieldSchema, ParsedWorkflow, ToolNode

# ---------------------------------------------------------------------------
# Plugin → tool_type registry
# ---------------------------------------------------------------------------

_PLUGIN_TO_TYPE: dict[str, str] = {
    "AlteryxBasePluginsGui.DbFileInput.DbFileInput": "db_file_input",
    "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput": "db_file_output",
    "AlteryxBasePluginsGui.OdbcInput.OdbcInput": "odbc_input",
    "AlteryxBasePluginsGui.OdbcOutput.OdbcOutput": "odbc_output",
    "AlteryxBasePluginsGui.Join.Join": "join",
    "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect": "select",
    "AlteryxBasePluginsGui.Formula.Formula": "formula",
    "AlteryxBasePluginsGui.MultiRowFormula.MultiRowFormula": "multi_row_formula",
    "AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula": "multi_field_formula",
    "AlteryxBasePluginsGui.TextInput.TextInput": "text_input",
    "AlteryxBasePluginsGui.Union.Union": "union",
    "AlteryxBasePluginsGui.Filter.Filter": "filter",
    "AlteryxBasePluginsGui.Unique.Unique": "unique",
    "AlteryxBasePluginsGui.RecordID.RecordID": "record_id",
    "AlteryxBasePluginsGui.Sort.Sort": "sort",
    "AlteryxBasePluginsGui.Sample.Sample": "sample",
    "AlteryxBasePluginsGui.AppendFields.AppendFields": "append_fields",
    "AlteryxBasePluginsGui.FindReplace.FindReplace": "find_replace",
    "AlteryxBasePluginsGui.BrowseV2.BrowseV2": "browse",
    "AlteryxSpatialPluginsGui.Summarize.Summarize": "summarize",
    "AlteryxGuiToolkit.ToolContainer.ToolContainer": "tool_container",
    "AlteryxGuiToolkit.Comment.Comment": "comment",
}

_CAMEL_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def _normalize_plugin(plugin: str) -> str:
    """Map a full plugin string to a short snake_case tool_type key.

    Falls back to snake_casing the last dot-segment of the plugin string
    after stripping a leading 'Alteryx' prefix, so unknown tools degrade
    gracefully rather than raising an error.
    """
    if plugin in _PLUGIN_TO_TYPE:
        return _PLUGIN_TO_TYPE[plugin]
    # Fallback: last segment, strip leading "Alteryx", CamelCase → snake_case
    last = plugin.split(".")[-1]
    last = re.sub(r"^Alteryx", "", last)
    return _CAMEL_RE.sub("_", last).lower()


# ---------------------------------------------------------------------------
# XML → Python dict conversion
# ---------------------------------------------------------------------------


def _elem_to_value(elem: ET.Element) -> dict[str, Any] | str:
    """Recursively convert an XML element into a Python dict or plain string.

    Rules:
    - Leaf elements with no attributes → return text content as str
    - Elements with attributes and/or children → return dict
    - Multiple sibling elements with the same tag → collapsed into a list
    - Inline text alongside attributes/children → stored under "_text" key
    """
    children = list(elem)
    attribs = dict(elem.attrib)
    text = (elem.text or "").strip()

    # Pure leaf — no attributes, no children
    if not children and not attribs:
        return text

    result: dict[str, Any] = dict(attribs)

    # Group children by tag name, collecting values into lists
    by_tag: dict[str, list[Any]] = {}
    for child in children:
        by_tag.setdefault(child.tag, []).append(_elem_to_value(child))

    for tag, values in by_tag.items():
        result[tag] = values[0] if len(values) == 1 else values

    # Preserve text content when it coexists with attributes/children
    if text:
        result["_text"] = text

    return result


# ---------------------------------------------------------------------------
# RecordInfo schema parsing
# ---------------------------------------------------------------------------


def _parse_record_info(properties_elem: ET.Element) -> list[FieldSchema]:
    """Extract column schemas from <MetaInfo>/<RecordInfo> inside <Properties>.

    Prefers the MetaInfo with connection="Output"; falls back to the first
    MetaInfo found. Returns an empty list if no RecordInfo is present.
    """
    meta = properties_elem.find('.//MetaInfo[@connection="Output"]')
    if meta is None:
        meta = properties_elem.find(".//MetaInfo")
    if meta is None:
        return []

    record_info = meta.find("RecordInfo")
    if record_info is None:
        return []

    fields: list[FieldSchema] = []
    for field_elem in record_info.findall("Field"):
        name = field_elem.get("name", "").strip()
        if not name:
            continue
        size_str = field_elem.get("size")
        fields.append(
            FieldSchema(
                name=name,
                alteryx_type=field_elem.get("type", ""),
                size=int(size_str) if size_str else None,
                source=field_elem.get("source"),
            )
        )
    return fields


# ---------------------------------------------------------------------------
# Source type inference
# ---------------------------------------------------------------------------

_FILE_EXTENSIONS: list[tuple[tuple[str, ...], str]] = [
    ((".csv", ".tsv", ".txt"), "CSV/Text File"),
    ((".xlsx", ".xlsm", ".xls"), "Excel File"),
    ((".yxdb",), "Alteryx Database"),
    ((".parquet",), "Parquet File"),
    ((".avro",), "Avro File"),
    ((".json",), "JSON File"),
    ((".xml",), "XML File"),
]


def _classify_connection_string(conn_str: str) -> str:
    """Return a human-readable source type from an Alteryx connection string."""
    s = conn_str.strip()
    if s.startswith("aka:"):
        return "SQL Database"
    if s.startswith(("s3://", "s3n://", "s3a://")):
        return "Amazon S3"
    if s.startswith("hdfs://"):
        return "HDFS"
    if s.startswith(("sftp://", "ftp://")):
        return "FTP/SFTP"
    if s.startswith(("http://", "https://")):
        return "HTTP"
    lower = s.lower().split("?")[0]
    for exts, label in _FILE_EXTENSIONS:
        if any(lower.endswith(e) for e in exts):
            return label
    return "File" if s else "Unknown"


def _infer_source_type(tool_type: str, config: dict) -> str | None:
    """Return the source type for input tools; None for all other tools."""
    if tool_type == "text_input":
        return "Inline Data"
    if tool_type == "odbc_input":
        return "ODBC Database"
    if tool_type == "db_file_input":
        file_cfg = config.get("File", {})
        conn_str = file_cfg.get("_text", "") if isinstance(file_cfg, dict) else str(file_cfg)
        return _classify_connection_string(conn_str)
    return None


# ---------------------------------------------------------------------------
# Node parsing
# ---------------------------------------------------------------------------


def _parse_node(node_elem: ET.Element) -> ToolNode:
    """Parse a single <Node> element into a ToolNode.

    Handles both regular tools (Plugin attribute on GuiSettings) and
    macro tools (EngineSettings Macro="filename.yxmc").
    """
    tool_id = int(node_elem.get("ToolID", 0))

    # --- Position ---
    pos_elem = node_elem.find(".//Position")
    if pos_elem is not None:
        x = int(pos_elem.get("x", 0))
        y = int(pos_elem.get("y", 0))
    else:
        x, y = 0, 0

    # --- Plugin / tool_type ---
    gui_settings = node_elem.find("GuiSettings")
    plugin = (gui_settings.get("Plugin") or "") if gui_settings is not None else ""

    engine_settings = node_elem.find("EngineSettings")
    macro_path: str | None = None
    if engine_settings is not None:
        macro_attr = engine_settings.get("Macro")
        if macro_attr:
            macro_path = macro_attr
            if not plugin:
                plugin = f"macro:{macro_attr}"

    tool_type = (
        "macro"
        if macro_path and not plugin.startswith("Alteryx")
        else _normalize_plugin(plugin)
    )

    # --- Configuration ---
    properties_elem = node_elem.find("Properties")
    config: dict[str, Any] = {}
    if properties_elem is not None:
        config_elem = properties_elem.find("Configuration")
        if config_elem is not None:
            raw = _elem_to_value(config_elem)
            config = raw if isinstance(raw, dict) else {}

    # --- Annotation ---
    annotation = ""
    if properties_elem is not None:
        ann_elem = properties_elem.find(".//DefaultAnnotationText")
        if ann_elem is not None and ann_elem.text:
            annotation = ann_elem.text.strip()

    # --- Output schema ---
    output_schema: list[FieldSchema] = []
    if properties_elem is not None:
        output_schema = _parse_record_info(properties_elem)

    return ToolNode(
        tool_id=tool_id,
        plugin=plugin,
        tool_type=tool_type,
        config=config,
        annotation=annotation,
        position=(x, y),
        output_schema=output_schema,
        macro_path=macro_path,
        source_type=_infer_source_type(tool_type, config),
    )


# ---------------------------------------------------------------------------
# Recursive node collection (handles ToolContainers with ChildNodes)
# ---------------------------------------------------------------------------


def _collect_nodes(parent_elem: ET.Element) -> list[ToolNode]:
    """Recursively collect ToolNodes, flattening any nested ChildNodes.

    ToolContainer nodes are included in the output (tool_type='tool_container')
    but their children are also extracted. The DAG builder skips containers.
    """
    result: list[ToolNode] = []
    for node_elem in parent_elem.findall("Node"):
        result.append(_parse_node(node_elem))
        child_nodes_elem = node_elem.find("ChildNodes")
        if child_nodes_elem is not None:
            result.extend(_collect_nodes(child_nodes_elem))
    return result


# ---------------------------------------------------------------------------
# Connection parsing
# ---------------------------------------------------------------------------

_ORDER_RE = re.compile(r"#(\d+)")


def _parse_connection(conn_elem: ET.Element) -> Connection | None:
    """Parse a single <Connection> element into a Connection model.

    Returns None if the element is missing Origin or Destination.
    """
    origin = conn_elem.find("Origin")
    dest = conn_elem.find("Destination")
    if origin is None or dest is None:
        return None

    origin_id_str = origin.get("ToolID")
    dest_id_str = dest.get("ToolID")
    if not origin_id_str or not dest_id_str:
        return None

    wireless = conn_elem.get("Wireless", "").lower() == "true"

    order: int | None = None
    name_attr = conn_elem.get("name", "")
    if m := _ORDER_RE.search(name_attr):
        order = int(m.group(1))

    return Connection(
        origin_id=int(origin_id_str),
        origin_anchor=origin.get("Connection", "Output"),
        dest_id=int(dest_id_str),
        dest_anchor=dest.get("Connection", "Input"),
        wireless=wireless,
        order=order,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def parse_workflow(path: Path | str) -> ParsedWorkflow:
    """Parse an Alteryx .yxmd file and return all nodes and connections.

    Args:
        path: Filesystem path to the .yxmd file.

    Returns:
        ParsedWorkflow with flat node list and connection list.

    Raises:
        FileNotFoundError: If the file does not exist.
        ET.ParseError: If the XML is malformed.
    """
    path = Path(path)
    tree = ET.parse(path)
    root = tree.getroot()

    # Nodes
    nodes_elem = root.find("Nodes")
    nodes: list[ToolNode] = []
    if nodes_elem is not None:
        nodes = _collect_nodes(nodes_elem)

    # Connections
    connections_elem = root.find("Connections")
    connections: list[Connection] = []
    if connections_elem is not None:
        for conn_elem in connections_elem.findall("Connection"):
            conn = _parse_connection(conn_elem)
            if conn is not None:
                connections.append(conn)

    return ParsedWorkflow(
        nodes=nodes,
        connections=connections,
        source_file=path.name,
    )
