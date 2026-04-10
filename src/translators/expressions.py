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
* DATETIMENOW()            → GETDATE()
* DATETIMETODAY()          → CAST(GETDATE() AS DATE)
* LEFT([col], n)           → LEFT([col], n)  — same in T-SQL
* RIGHT([col], n)          → RIGHT([col], n)
* SUBSTRING([col], s, l)   → SUBSTRING([col], s, l)  — same
* REPLACE([c], [f], [t])   → REPLACE([c], [f], [t])  — same
* ABS / CEILING / FLOOR / ROUND / SQRT — same in T-SQL; passed through
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Patterns that indicate LLM is required
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

# Simple function-name renames (Alteryx → T-SQL), applied via regex substitution.
# Order matters: longer / more specific patterns first.
_FUNCTION_RENAMES: list[tuple[re.Pattern[str], str]] = [
    # Alteryx          → T-SQL
    (re.compile(r"\bLENGTH\s*\(", re.IGNORECASE), "LEN("),
    (re.compile(r"\bUPPERCASE\s*\(", re.IGNORECASE), "UPPER("),
    (re.compile(r"\bLOWERCASE\s*\(", re.IGNORECASE), "LOWER("),
    (re.compile(r"\bDATETIMENOW\s*\(\s*\)", re.IGNORECASE), "GETDATE()"),
    (
        re.compile(r"\bDATETIMETODAY\s*\(\s*\)", re.IGNORECASE),
        "CAST(GETDATE() AS DATE)",
    ),
]

# Compound patterns — require capture groups
_COMPOUND_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # CONTAINS([col], "val") → [col] LIKE '%val%'
    (
        re.compile(
            r"\bCONTAINS\s*\(\s*(\[?[\w ]+\]?)\s*,\s*\"([^\"]*)\"\s*\)", re.IGNORECASE
        ),
        r"\1 LIKE '%\2%'",
    ),
    # STARTSWITH([col], "val") → [col] LIKE 'val%'
    (
        re.compile(
            r"\bSTARTSWITH\s*\(\s*(\[?[\w ]+\]?)\s*,\s*\"([^\"]*)\"\s*\)", re.IGNORECASE
        ),
        r"\1 LIKE '\2%'",
    ),
    # ISNULL([col])  → [col] IS NULL
    (
        re.compile(r"\bISNULL\s*\(\s*(\[?[\w ]+\]?)\s*\)", re.IGNORECASE),
        r"\1 IS NULL",
    ),
    # ISNOTNULL([col]) → [col] IS NOT NULL
    (
        re.compile(r"\bISNOTNULL\s*\(\s*(\[?[\w ]+\]?)\s*\)", re.IGNORECASE),
        r"\1 IS NOT NULL",
    ),
    # TRIM([col])  → LTRIM(RTRIM([col]))
    (
        re.compile(r"\bTRIM\s*\(\s*(\[?[\w ]+\]?)\s*\)", re.IGNORECASE),
        r"LTRIM(RTRIM(\1))",
    ),
    # TONUMBER([col]) → TRY_CAST([col] AS FLOAT)
    (
        re.compile(r"\bTONUMBER\s*\(\s*(\[?[\w ]+\]?)\s*\)", re.IGNORECASE),
        r"TRY_CAST(\1 AS FLOAT)",
    ),
    # TOSTRING([col]) → CAST([col] AS NVARCHAR(MAX))
    (
        re.compile(r"\bTOSTRING\s*\(\s*(\[?[\w ]+\]?)\s*\)", re.IGNORECASE),
        r"CAST(\1 AS NVARCHAR(MAX))",
    ),
]


def convert_expression(expression: str) -> str:
    """Convert an Alteryx expression string to its T-SQL equivalent.

    This is a best-effort deterministic converter.  Always call
    `needs_llm_translation()` first; only call this when it returns False.
    """
    expr = expression

    # 1. Compound patterns first — they match double-quoted string literals
    #    inside function calls (e.g. CONTAINS([col], "val")).  Must run before
    #    the global double-quote → single-quote conversion below.
    for pattern, replacement in _COMPOUND_PATTERNS:
        expr = pattern.sub(replacement, expr)

    # 2. Double-quoted string literals → single-quoted (remaining literals)
    expr = re.sub(
        r'"([^"]*)"', lambda m: "'" + m.group(1).replace("'", "''") + "'", expr
    )

    # 3. [Null] → NULL
    expr = re.sub(r"\[Null\]", "NULL", expr, flags=re.IGNORECASE)

    # 4. Boolean literals
    expr = re.sub(r"\bTrue\b", "1", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bFalse\b", "0", expr, flags=re.IGNORECASE)

    # 5. Simple function renames
    for pattern, replacement in _FUNCTION_RENAMES:
        expr = pattern.sub(replacement, expr)

    return expr
