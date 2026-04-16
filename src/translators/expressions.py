"""Alteryx expression → T-SQL expression converter (deterministic subset).

This module handles the straightforward cases.  Complex expressions — those
containing IF...THEN...ELSE, REGEX functions, custom Alteryx functions, or
multi-line logic — are flagged with `needs_llm_translation()` so the Phase 4
LLM agent can take over.

Alteryx expression syntax reference (subset handled here)
----------------------------------------------------------
* Literals:  "text" → 'text'  (double-quoted strings to single-quoted)
* Null:      [Null] → NULL
* Booleans:  True / False (case-insensitive) → 1 / 0  (MSSQL has no bool)
* Arithmetic operators: + - * / % — passed through unchanged
* Comparison operators: = != < > <= >= — passed through
             != is already valid T-SQL
* Logical:   AND OR NOT — passed through (Alteryx uses same keywords)
* CONTAINS([col], "val")   → [col] LIKE '%val%'
* STARTSWITH([col], "val") → [col] LIKE 'val%'
* ISNULL([col])            → [col] IS NULL
* ISNOTNULL([col])         → [col] IS NOT NULL
* ISNUMERIC([col])         → ISNUMERIC([col])  (T-SQL native)
* TRIM([col])              → LTRIM(RTRIM([col]))
* LENGTH([col])            → LEN([col])
* UPPERCASE([col])         → UPPER([col])
* LOWERCASE([col])         → LOWER([col])
* TONUMBER([col])          → TRY_CAST([col] AS FLOAT)
* TOSTRING([col])          → CAST([col] AS NVARCHAR(MAX))
* TODATE([col])            → CAST([col] AS DATE)
* ISINTEGER([col])         → TRY_CAST([col] AS INT) IS NOT NULL
* TITLECASE([col])         → UPPER(LEFT([col],1)) + LOWER(SUBSTRING([col],2,LEN([col])))
                              (first word only; full title case requires a UDF)
* DATETIMENOW()            → GETDATE()
* DATETIMETODAY()          → CAST(GETDATE() AS DATE)
* DATETIMEFIRSTOFMONTH()   → DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
* LEFT([col], n)           → LEFT([col], n)  — same in T-SQL
* RIGHT([col], n)          → RIGHT([col], n)
* SUBSTRING([col], s, l)   → SUBSTRING([col], s, l)  — same
* REPLACE([c], [f], [t])   → REPLACE([c], [f], [t])  — same
* ABS / CEILING / FLOOR / ROUND / SQRT — same in T-SQL; passed through
* [Engine.XYZ]             → @XYZ  (Alteryx engine variable → SQL variable)
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Patterns that indicate LLM is required
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# SQL prose detection
# ---------------------------------------------------------------------------

_PROSE_RE = re.compile(
    r"(?:"
    r"\.\s+[A-Z][a-z]"  # sentence boundary: ". Capital word"
    r"|(?:^|\s)(?:This|The|In\s|Note:|I\s|It\s|You\s|However|Additionally|To\s+translate)\b"
    r")",
    re.IGNORECASE | re.MULTILINE,
)

_SQL_TOKEN_RE = re.compile(
    r"[\[\]=<>]|CAST\s*\(|CASE\b|WHEN\b|CONVERT\s*\(|ISNULL\s*\(|"
    r"SELECT\b|WHERE\b|LIKE\b|NULL\b|TRY_CAST\s*\(|COALESCE\s*\(",
    re.IGNORECASE,
)


def looks_like_sql(text: str) -> bool:
    """Return True if *text* looks like a SQL expression rather than English prose.

    Used to validate LLM output from the expression agent.  A response that
    reads as a sentence-based explanation rather than a SQL expression should
    be treated as a failed translation and stubbed out.
    """
    if _PROSE_RE.search(text):
        return False
    # A long response with no SQL-like tokens is highly suspicious
    if len(text.split()) > 12 and not _SQL_TOKEN_RE.search(text):
        return False
    return True


# ---------------------------------------------------------------------------
# Date-in-string-concatenation fix
# ---------------------------------------------------------------------------

# Matches CAST(... AS DATE/DATETIME/DATETIME2) allowing one level of nested parens
# (needed for e.g. CAST(GETDATE() AS DATE)).
_DATE_CAST_RE = re.compile(
    r"CAST\s*\("
    r"(?:[^()]*|\([^()]*\))*"
    r"\s+AS\s+(?:DATE|DATETIME2?(?:\s*\(\s*\d+\s*\))?)\s*\)",
    re.IGNORECASE,
)


def _fix_date_string_concat(expr: str) -> str:
    """Wrap date-typed CAST expressions in CONVERT(NVARCHAR) when used with string +.

    In T-SQL, ``'string' + CAST(x AS DATE)`` fails because the engine tries to
    convert the string literal to a date rather than the date to a string.
    Wrapping with ``CONVERT(NVARCHAR(50), ..., 120)`` produces a formatted date
    string (YYYY-MM-DD HH:MM:SS) and allows the concatenation to succeed.

    Only applied when the expression contains both ``+`` and a string literal,
    so pure arithmetic or comparison expressions are left untouched.
    """
    if "+" not in expr or not re.search(r"'[^']*'", expr):
        return expr
    return _DATE_CAST_RE.sub(lambda m: f"CONVERT(NVARCHAR(50), {m.group(0)}, 120)", expr)


# ---------------------------------------------------------------------------

_LLM_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bIF\b", re.IGNORECASE),
    re.compile(r"\bREGEX_", re.IGNORECASE),
    re.compile(r"\bSWITCH\s*\(", re.IGNORECASE),
    re.compile(r"\bIIF\s*\(", re.IGNORECASE),
    re.compile(r"\bFINDSTRING\s*\(", re.IGNORECASE),
    re.compile(r"\bGETSELECTEDFIELD\s*\(", re.IGNORECASE),
    re.compile(r"\bDATETIMEADD\s*\(", re.IGNORECASE),
    re.compile(r"\bDATETIMEDIFF\s*\(", re.IGNORECASE),
    re.compile(r"\bDATETIMEFORMAT\s*\(", re.IGNORECASE),
    re.compile(r"\bDATETIMEPARSE\s*\(", re.IGNORECASE),
    re.compile(r"\bDATETIMETRIM\s*\(", re.IGNORECASE),
    re.compile(r"\bDATE\s*\(", re.IGNORECASE),
    re.compile(r"\bSTRING_TO\s*\(", re.IGNORECASE),
    re.compile(r"\bMESSAGEBOX\s*\(", re.IGNORECASE),
    re.compile(r"\bERROR\s*\(", re.IGNORECASE),
]


def needs_llm_translation(expression: str) -> bool:
    """Return True if the expression contains constructs we cannot deterministically convert."""
    return any(p.search(expression) for p in _LLM_PATTERNS)


# ---------------------------------------------------------------------------
# Deterministic conversion
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Engine variable replacement
# ---------------------------------------------------------------------------

_ENGINE_VAR_RE = re.compile(r"\[Engine\.([^\]]+)\]", re.IGNORECASE)


def _replace_engine_vars(expr: str, engine_vars: set[str] | None) -> str:
    """Replace Alteryx [Engine.XYZ] references with @XYZ SQL variables.

    Collected variable names are added to *engine_vars* (if provided) so the
    caller can emit the corresponding DECLARE statements in the procedure body.
    """

    def _sub(m: re.Match) -> str:
        raw = m.group(1)
        var_name = re.sub(r"[^A-Za-z0-9_]", "_", raw).strip("_")
        if engine_vars is not None:
            engine_vars.add(var_name)
        return f"@{var_name}"

    return _ENGINE_VAR_RE.sub(_sub, expr)


# ---------------------------------------------------------------------------
# Function rename tables
# ---------------------------------------------------------------------------

# Simple function-name renames (Alteryx → T-SQL), applied via regex substitution.
# Order matters: longer / more specific patterns first.
_FUNCTION_RENAMES: list[tuple[re.Pattern[str], str]] = [
    # Alteryx                    → T-SQL
    (re.compile(r"\bLENGTH\s*\(", re.IGNORECASE), "LEN("),
    (re.compile(r"\bUPPERCASE\s*\(", re.IGNORECASE), "UPPER("),
    (re.compile(r"\bLOWERCASE\s*\(", re.IGNORECASE), "LOWER("),
    (re.compile(r"\bDATETIMENOW\s*\(\s*\)", re.IGNORECASE), "GETDATE()"),
    (re.compile(r"\bDATETIMETODAY\s*\(\s*\)", re.IGNORECASE), "CAST(GETDATE() AS DATE)"),
    (
        re.compile(r"\bDATETIMEFIRSTOFMONTH\s*\(\s*\)", re.IGNORECASE),
        "DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)",
    ),
]

# Compound patterns — require capture groups.
# _COL matches a simple column reference: optional [ word-chars ] or bare word-chars.
_COL = r"(\[?[\w ]+\]?)"

_COMPOUND_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # CONTAINS([col], "val") → [col] LIKE '%val%'
    (
        re.compile(r"\bCONTAINS\s*\(\s*" + _COL + r"\s*,\s*\"([^\"]*)\"\s*\)", re.IGNORECASE),
        r"\1 LIKE '%\2%'",
    ),
    # STARTSWITH([col], "val") → [col] LIKE 'val%'
    (
        re.compile(r"\bSTARTSWITH\s*\(\s*" + _COL + r"\s*,\s*\"([^\"]*)\"\s*\)", re.IGNORECASE),
        r"\1 LIKE '\2%'",
    ),
    # ISNULL([col])  → [col] IS NULL
    (
        re.compile(r"\bISNULL\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"\1 IS NULL",
    ),
    # ISNOTNULL([col]) → [col] IS NOT NULL
    (
        re.compile(r"\bISNOTNULL\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"\1 IS NOT NULL",
    ),
    # ISINTEGER([col]) → TRY_CAST([col] AS INT) IS NOT NULL
    (
        re.compile(r"\bISINTEGER\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"TRY_CAST(\1 AS INT) IS NOT NULL",
    ),
    # TRIM([col])  → LTRIM(RTRIM([col]))
    (
        re.compile(r"\bTRIM\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"LTRIM(RTRIM(\1))",
    ),
    # TONUMBER([col]) → TRY_CAST([col] AS FLOAT)
    (
        re.compile(r"\bTONUMBER\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"TRY_CAST(\1 AS FLOAT)",
    ),
    # TOSTRING([col]) → CAST([col] AS NVARCHAR(MAX))
    (
        re.compile(r"\bTOSTRING\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"CAST(\1 AS NVARCHAR(MAX))",
    ),
    # TODATE([col]) → CAST([col] AS DATE)
    (
        re.compile(r"\bTODATE\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"CAST(\1 AS DATE)",
    ),
    # TITLECASE([col]) → UPPER(LEFT([col],1)) + LOWER(SUBSTRING([col],2,LEN([col])))
    # Note: only title-cases the first character; full per-word title case needs a UDF.
    (
        re.compile(r"\bTITLECASE\s*\(\s*" + _COL + r"\s*\)", re.IGNORECASE),
        r"UPPER(LEFT(\1, 1)) + LOWER(SUBSTRING(\1, 2, LEN(\1)))",
    ),
]


def convert_expression(expression: str, engine_vars: set[str] | None = None) -> str:
    """Convert an Alteryx expression string to its T-SQL equivalent.

    Args:
        expression:  Raw Alteryx formula expression.
        engine_vars: If provided, any [Engine.XYZ] variable names found are
                     added to this set so the caller can emit DECLARE statements.

    This is a best-effort deterministic converter.  Always call
    `needs_llm_translation()` first; only call this when it returns False.
    """
    expr = expression

    # 0. Replace Alteryx engine variables ([Engine.XYZ] → @XYZ) before any
    #    other substitution so they don't get mangled by the bracket handling.
    expr = _replace_engine_vars(expr, engine_vars)

    # 1. Compound patterns first — they match double-quoted string literals
    #    inside function calls (e.g. CONTAINS([col], "val")).  Must run before
    #    the global double-quote → single-quote conversion below.
    for pattern, replacement in _COMPOUND_PATTERNS:
        expr = pattern.sub(replacement, expr)

    # 2. Double-quoted string literals → single-quoted (remaining literals)
    expr = re.sub(r'"([^"]*)"', lambda m: "'" + m.group(1).replace("'", "''") + "'", expr)

    # 3. [Null] → NULL
    expr = re.sub(r"\[Null\]", "NULL", expr, flags=re.IGNORECASE)

    # 4. Boolean literals
    expr = re.sub(r"\bTrue\b", "1", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bFalse\b", "0", expr, flags=re.IGNORECASE)

    # 5. Simple function renames
    for pattern, replacement in _FUNCTION_RENAMES:
        expr = pattern.sub(replacement, expr)

    # 6. Wrap date casts in CONVERT(NVARCHAR) when used in string concatenation.
    #    Must run after function renames so DATETIMETODAY() → CAST(GETDATE() AS DATE)
    #    is already in place before this step inspects the expression.
    expr = _fix_date_string_concat(expr)

    return expr
