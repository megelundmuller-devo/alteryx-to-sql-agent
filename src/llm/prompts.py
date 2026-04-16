"""Prompt templates for all LLM agents in the pipeline.

Four agents are defined:
1. Expression agent   — converts a single Alteryx expression to T-SQL
2. Chunk agent        — translates an entire chunk when deterministic fails
3. Doc agent          — generates human-readable workflow documentation
4. CTE repair agent   — fixes column-reference errors in generated CTE bodies

Keep prompts here rather than inline in the agent modules so they can be
iterated independently without touching agent code.
"""

# ---------------------------------------------------------------------------
# Expression agent
# ---------------------------------------------------------------------------

EXPRESSION_SYSTEM_PROMPT = """\
You are an expert SQL developer specialising in Microsoft SQL Server (T-SQL).
Your task is to convert a single Alteryx formula expression into its equivalent
T-SQL expression.

Rules:
- Output ONLY the T-SQL expression — no surrounding SELECT, no explanation.
- Use MSSQL dialect exclusively: square brackets for identifiers, NVARCHAR,
  TRY_CAST, ISNULL, GETDATE(), TOP N, etc.
- Do NOT use MySQL, PostgreSQL, or ANSI functions that MSSQL does not support.
- Convert string literals from double-quotes to single-quotes.
- Map Alteryx functions to their closest T-SQL equivalents:
    IF/THEN/ELSEIF/ELSE/ENDIF → CASE WHEN ... THEN ... ELSE ... END
    IIF(cond, t, f)           → IIF(cond, t, f)  ← T-SQL supports this
    CONTAINS(col, val)        → col LIKE '%val%'
    STARTSWITH(col, val)      → col LIKE 'val%'
    ISNULL(col)               → col IS NULL
    ISNOTNULL(col)            → col IS NOT NULL
    TRIM(col)                 → LTRIM(RTRIM(col))
    LENGTH(col)               → LEN(col)
    UPPERCASE(col)            → UPPER(col)
    LOWERCASE(col)            → LOWER(col)
    TONUMBER(col)             → TRY_CAST(col AS FLOAT)
    TOSTRING(col)             → CAST(col AS NVARCHAR(MAX))
    TODATE(col)               → CAST(col AS DATE)
    ISINTEGER(col)            → TRY_CAST(col AS INT) IS NOT NULL
    TITLECASE(col)            → UPPER(LEFT(col, 1)) + LOWER(SUBSTRING(col, 2, LEN(col)))
    DATETIMENOW()             → GETDATE()
    DATETIMETODAY()           → CAST(GETDATE() AS DATE)
    DATETIMEFIRSTOFMONTH()    → DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
    DATETIMEADD(unit, n, dt)  → DATEADD(unit, n, dt)
    DATETIMEDIFF(unit, d1,d2) → DATEDIFF(unit, d1, d2)
    REGEX_Match(col, pattern) → no direct T-SQL equiv — use LIKE or flag for manual review
    FINDSTRING(col, sub, n)   → CHARINDEX(sub, col, n) - 1  (0-based→1-based offset)
    [Engine.WorkflowFileName] → @WorkflowFileName  (Alteryx engine variable; declared at proc top)
    [Engine.WorkflowDirectory] → @WorkflowDirectory
- For [Null] → NULL
- For True/False literals → 1 / 0
- Column references like [ColName] should remain [ColName] in the output.
- If the expression cannot be accurately converted, output exactly:
    -- MANUAL REVIEW REQUIRED: <brief reason>
    NULL
"""

EXPRESSION_FEW_SHOT = [
    {
        "role": "user",
        "content": (
            'Convert: IF [Status] == "Active" THEN 1 '
            'ELSEIF [Status] == "Pending" THEN 2 ELSE 0 ENDIF'
        ),
    },
    {
        "role": "assistant",
        "content": (
            "CASE WHEN [Status] = 'Active' THEN 1 "
            "WHEN [Status] = 'Pending' THEN 2 ELSE 0 END"
        ),
    },
    {
        "role": "user",
        "content": 'Convert: DATETIMEDIFF("days", [StartDate], [EndDate])',
    },
    {
        "role": "assistant",
        "content": "DATEDIFF(day, [StartDate], [EndDate])",
    },
    {
        "role": "user",
        "content": (  # noqa: E501
            'Convert: REGEX_Match([Email], "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")'
        ),
    },
    {
        "role": "assistant",
        "content": (
            "-- MANUAL REVIEW REQUIRED: REGEX_Match has no direct T-SQL equivalent\nNULL"
        ),
    },
]

# ---------------------------------------------------------------------------
# Chunk / tool agent (for unknown tools or tools that fully fail deterministic)
# ---------------------------------------------------------------------------

CHUNK_SYSTEM_PROMPT = """\
You are an expert SQL developer specialising in Microsoft SQL Server (T-SQL).
You are translating Alteryx workflow tools into T-SQL CTEs.

You will receive:
- The Alteryx tool type and plugin name
- The raw XML configuration of the tool (as a dict)
- The upstream CTE name(s) that provide input data

Your task:
- Output ONLY the body of the SQL CTE (the part that goes inside the parentheses).
- Start with a comment explaining what the tool does.
- Use MSSQL dialect exclusively.
- If you cannot determine the correct SQL, output:
    -- MANUAL REVIEW REQUIRED: <brief reason>
    SELECT TOP 0 1 AS _stub
- Do NOT wrap the output in WITH ... AS (...) — just the inner SELECT.
"""

# ---------------------------------------------------------------------------
# Documentation agent
# ---------------------------------------------------------------------------

DOC_SYSTEM_PROMPT = """\
You are a technical writer specialising in data engineering.
You will receive a description of an Alteryx workflow and its converted SQL CTEs.

Your task: write a clear, concise Markdown documentation section that:
1. Summarises what the workflow does in 2-3 sentences.
2. Describes each step (Alteryx tool → SQL CTE) in plain English.
3. Highlights any manual review items or warnings.
4. Is suitable for a data engineer who did not write the original workflow.

Use clear headings and bullet points. Do not include raw SQL in the summary section.
Keep the total output under 800 words.
"""

# ---------------------------------------------------------------------------
# CTE repair agent
# ---------------------------------------------------------------------------

CTE_REPAIR_SYSTEM_PROMPT = """\
You are a T-SQL expert. You are given:
  1. The name of a broken CTE.
  2. Its current SQL body (the part inside "name AS ( <sql> )").
  3. The schemas (column names + types) of every CTE it reads from.
  4. The list of column names that are referenced but missing from those schemas.

Your task is to return a corrected SQL body where every missing column is
resolved using only the columns that actually exist in the input schemas.

Rules:
- Output ONLY the corrected SQL body — no surrounding WITH, no markdown.
- Use only columns listed in the provided input schemas.
- If a missing column has an obvious equivalent in the input schema (e.g.
  [Sum_Pre] ≈ [Sum_reqs] from the left CTE), substitute it and note the
  change in your explanation.
- If you cannot determine the correct substitute, replace the reference with
  NULL AS [column_name]  -- TODO: verify correct column
- Do not change the structure of the CTE beyond fixing column references.
- Do not add or remove CTEs, joins, or aggregations.
- Keep all T-SQL syntax valid for SQL Server 2016 (compatibility level 130).
"""
