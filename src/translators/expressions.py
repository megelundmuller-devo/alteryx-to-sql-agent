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
* ABS / CEILING / FLOOR / SQRT — same in T-SQL; passed through
* ROUND(x, 0.01)             → ROUND(x, 2)  (Alteryx 2nd arg is a multiple, T-SQL is dp count)
* [Engine.XYZ]             → @XYZ  (Alteryx engine variable → SQL variable)
"""

from __future__ import annotations

import math
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
    re.compile(r"\bREGEX_", re.IGNORECASE),
    re.compile(r"\bGETSELECTEDFIELD\s*\(", re.IGNORECASE),
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


_ROUND_CALL_RE = re.compile(r"\bROUND\s*\(", re.IGNORECASE)
_NUMERIC_LITERAL_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)$")


def _convert_round_calls(expr: str) -> str:
    """Convert Alteryx ROUND(x, mult) → T-SQL ROUND(x, decimal_places).

    Alteryx's second arg is a rounding multiple (0.01 = nearest 0.01, i.e. 2 dp).
    T-SQL's second arg is the number of decimal places directly.
    Conversion: decimal_places = int(round(-log10(mult))).

    If the second arg is not a numeric literal the call is left unchanged.
    """
    result: list[str] = []
    pos = 0
    for m in _ROUND_CALL_RE.finditer(expr):
        result.append(expr[pos : m.start()])
        start = m.end() - 1  # index of the opening '('
        depth = 0
        last_comma = -1
        end = start
        for i in range(start, len(expr)):
            c = expr[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
            elif c == "," and depth == 1:
                last_comma = i

        if last_comma == -1 or end <= last_comma:
            # Malformed or single-arg call — pass through unchanged.
            result.append(expr[m.start() : end + 1])
            pos = end + 1
            continue

        val_part = expr[start + 1 : last_comma]
        mult_str = expr[last_comma + 1 : end].strip()

        if _NUMERIC_LITERAL_RE.match(mult_str):
            try:
                mult = float(mult_str)
                if mult > 0:
                    dp = int(round(-math.log10(mult)))
                    result.append(f"ROUND({val_part}, {dp})")
                else:
                    result.append(f"ROUND({val_part}, {mult_str})")
            except (ValueError, OverflowError):
                result.append(f"ROUND({val_part}, {mult_str})")
        else:
            result.append(f"ROUND({val_part}, {mult_str})")
        pos = end + 1

    result.append(expr[pos:])
    return "".join(result)


def _read_balanced_parens(expr: str, open_idx: int) -> tuple[str, int] | None:
    """Return (inner_text, close_idx) for a balanced (...) group starting at open_idx."""
    if open_idx >= len(expr) or expr[open_idx] != "(":
        return None
    depth = 0
    in_single = False
    in_double = False
    i = open_idx
    while i < len(expr):
        ch = expr[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(expr) and expr[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue
        if in_single or in_double:
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return expr[open_idx + 1 : i], i
        i += 1
    return None


def _split_top_level_args(text: str) -> list[str]:
    """Split a comma-separated argument list, respecting nested groups/quotes."""
    parts: list[str] = []
    start = 0
    depth = 0
    in_single = False
    in_double = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue
        if in_single or in_double:
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
        i += 1
    parts.append(text[start:].strip())
    return parts


_IIF_START_RE = re.compile(r"\bIIF\s*\(", re.IGNORECASE)
_SWITCH_START_RE = re.compile(r"\bSWITCH\s*\(", re.IGNORECASE)
_FINDSTRING_START_RE = re.compile(r"\bFINDSTRING\s*\(", re.IGNORECASE)
_DATETIMEADD_START_RE = re.compile(r"\bDATETIMEADD\s*\(", re.IGNORECASE)
_DATETIMEDIFF_START_RE = re.compile(r"\bDATETIMEDIFF\s*\(", re.IGNORECASE)


def _replace_iif_calls(expr: str, engine_vars: set[str] | None) -> str:
    """Rewrite IIF(cond, a, b) calls into CASE expressions."""
    out: list[str] = []
    cursor = 0
    for m in _IIF_START_RE.finditer(expr):
        out.append(expr[cursor : m.start()])
        open_idx = m.end() - 1
        parsed = _read_balanced_parens(expr, open_idx)
        if parsed is None:
            out.append(expr[m.start() : m.end()])
            cursor = m.end()
            continue
        inner, close_idx = parsed
        args = _split_top_level_args(inner)
        if len(args) != 3:
            out.append(expr[m.start() : close_idx + 1])
            cursor = close_idx + 1
            continue
        cond = _convert_expression_impl(args[0], engine_vars)
        true_expr = _convert_expression_impl(args[1], engine_vars)
        false_expr = _convert_expression_impl(args[2], engine_vars)
        out.append(f"(CASE WHEN {cond} THEN {true_expr} ELSE {false_expr} END)")
        cursor = close_idx + 1
    out.append(expr[cursor:])
    return "".join(out)


def _replace_switch_calls(expr: str, engine_vars: set[str] | None) -> str:
    """Rewrite SWITCH(value, default, case1, result1, ...) into CASE expressions."""
    out: list[str] = []
    cursor = 0
    for m in _SWITCH_START_RE.finditer(expr):
        out.append(expr[cursor : m.start()])
        open_idx = m.end() - 1
        parsed = _read_balanced_parens(expr, open_idx)
        if parsed is None:
            out.append(expr[m.start() : m.end()])
            cursor = m.end()
            continue
        inner, close_idx = parsed
        args = _split_top_level_args(inner)
        if len(args) < 4 or len(args) % 2 != 0:
            out.append(expr[m.start() : close_idx + 1])
            cursor = close_idx + 1
            continue
        switch_value = _convert_expression_impl(args[0], engine_vars)
        default_expr = _convert_expression_impl(args[1], engine_vars)
        when_parts: list[str] = []
        for i in range(2, len(args), 2):
            case_expr = _convert_expression_impl(args[i], engine_vars)
            result_expr = _convert_expression_impl(args[i + 1], engine_vars)
            when_parts.append(f"WHEN {case_expr} THEN {result_expr}")
        out.append(f"(CASE {switch_value} {' '.join(when_parts)} ELSE {default_expr} END)")
        cursor = close_idx + 1
    out.append(expr[cursor:])
    return "".join(out)


def _replace_findstring_calls(expr: str, engine_vars: set[str] | None) -> str:
    """Rewrite FINDSTRING(text, target[, start]) to CHARINDEX(target, text[, start])."""
    out: list[str] = []
    cursor = 0
    for m in _FINDSTRING_START_RE.finditer(expr):
        out.append(expr[cursor : m.start()])
        open_idx = m.end() - 1
        parsed = _read_balanced_parens(expr, open_idx)
        if parsed is None:
            out.append(expr[m.start() : m.end()])
            cursor = m.end()
            continue
        inner, close_idx = parsed
        args = _split_top_level_args(inner)
        if len(args) not in (2, 3):
            out.append(expr[m.start() : close_idx + 1])
            cursor = close_idx + 1
            continue
        text_expr = _convert_expression_impl(args[0], engine_vars)
        target_expr = _convert_expression_impl(args[1], engine_vars)
        if len(args) == 2:
            out.append(f"CHARINDEX({target_expr}, {text_expr})")
        else:
            start_expr = _convert_expression_impl(args[2], engine_vars)
            out.append(f"CHARINDEX({target_expr}, {text_expr}, {start_expr})")
        cursor = close_idx + 1
    out.append(expr[cursor:])
    return "".join(out)


def _unwrap_quoted_literal(text: str) -> str | None:
    """Return unquoted string when text is a quoted literal, else None."""
    s = text.strip()
    if len(s) >= 2 and ((s[0] == "'" and s[-1] == "'") or (s[0] == '"' and s[-1] == '"')):
        return s[1:-1]
    return None


_DATEPART_MAP: dict[str, str] = {
    "year": "year",
    "years": "year",
    "month": "month",
    "months": "month",
    "day": "day",
    "days": "day",
    "week": "week",
    "weeks": "week",
    "hour": "hour",
    "hours": "hour",
    "minute": "minute",
    "minutes": "minute",
    "second": "second",
    "seconds": "second",
    "millisecond": "millisecond",
    "milliseconds": "millisecond",
    "quarter": "quarter",
    "quarters": "quarter",
}


def _replace_datetimeadd_calls(expr: str, engine_vars: set[str] | None) -> str:
    """Rewrite DATETIMEADD(unit, n, date) or DATETIMEADD(date, n, unit) to DATEADD."""
    out: list[str] = []
    cursor = 0
    for m in _DATETIMEADD_START_RE.finditer(expr):
        out.append(expr[cursor : m.start()])
        open_idx = m.end() - 1
        parsed = _read_balanced_parens(expr, open_idx)
        if parsed is None:
            out.append(expr[m.start() : m.end()])
            cursor = m.end()
            continue
        inner, close_idx = parsed
        args = _split_top_level_args(inner)
        if len(args) != 3:
            out.append(expr[m.start() : close_idx + 1])
            cursor = close_idx + 1
            continue

        first_lit = _unwrap_quoted_literal(args[0])
        third_lit = _unwrap_quoted_literal(args[2])

        if first_lit is not None and first_lit.lower() in _DATEPART_MAP:
            datepart = _DATEPART_MAP[first_lit.lower()]
            interval_expr = _convert_expression_impl(args[1], engine_vars)
            date_expr = _convert_expression_impl(args[2], engine_vars)
            out.append(f"DATEADD({datepart}, {interval_expr}, {date_expr})")
        elif third_lit is not None and third_lit.lower() in _DATEPART_MAP:
            datepart = _DATEPART_MAP[third_lit.lower()]
            date_expr = _convert_expression_impl(args[0], engine_vars)
            interval_expr = _convert_expression_impl(args[1], engine_vars)
            out.append(f"DATEADD({datepart}, {interval_expr}, {date_expr})")
        else:
            out.append(expr[m.start() : close_idx + 1])

        cursor = close_idx + 1
    out.append(expr[cursor:])
    return "".join(out)


def _replace_datetimediff_calls(expr: str, engine_vars: set[str] | None) -> str:
    """Rewrite DATETIMEDIFF(unit, start, end) or DATETIMEDIFF(start, end, unit) to DATEDIFF."""
    out: list[str] = []
    cursor = 0
    for m in _DATETIMEDIFF_START_RE.finditer(expr):
        out.append(expr[cursor : m.start()])
        open_idx = m.end() - 1
        parsed = _read_balanced_parens(expr, open_idx)
        if parsed is None:
            out.append(expr[m.start() : m.end()])
            cursor = m.end()
            continue
        inner, close_idx = parsed
        args = _split_top_level_args(inner)
        if len(args) != 3:
            out.append(expr[m.start() : close_idx + 1])
            cursor = close_idx + 1
            continue

        first_lit = _unwrap_quoted_literal(args[0])
        third_lit = _unwrap_quoted_literal(args[2])

        if first_lit is not None and first_lit.lower() in _DATEPART_MAP:
            datepart = _DATEPART_MAP[first_lit.lower()]
            start_expr = _convert_expression_impl(args[1], engine_vars)
            end_expr = _convert_expression_impl(args[2], engine_vars)
            out.append(f"DATEDIFF({datepart}, {start_expr}, {end_expr})")
        elif third_lit is not None and third_lit.lower() in _DATEPART_MAP:
            datepart = _DATEPART_MAP[third_lit.lower()]
            start_expr = _convert_expression_impl(args[0], engine_vars)
            end_expr = _convert_expression_impl(args[1], engine_vars)
            out.append(f"DATEDIFF({datepart}, {start_expr}, {end_expr})")
        else:
            out.append(expr[m.start() : close_idx + 1])

        cursor = close_idx + 1
    out.append(expr[cursor:])
    return "".join(out)


def _parse_if_then_else(expr: str) -> tuple[str, str, str] | None:
    """Parse IF ... THEN ... ELSE ... ENDIF block at expression root."""
    stripped = expr.strip()
    if not stripped[:2].upper() == "IF" or (len(stripped) > 2 and stripped[2].isalnum()):
        return None

    i = 0
    depth = 0
    in_single = False
    in_double = False
    then_pos = -1
    else_pos = -1
    endif_pos = -1

    def _is_word_at(pos: int, word: str) -> bool:
        end = pos + len(word)
        if end > len(stripped):
            return False
        if stripped[pos:end].upper() != word:
            return False
        before_ok = pos == 0 or not (stripped[pos - 1].isalnum() or stripped[pos - 1] == "_")
        after_ok = end == len(stripped) or not (stripped[end].isalnum() or stripped[end] == "_")
        return before_ok and after_ok

    while i < len(stripped):
        ch = stripped[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(stripped) and stripped[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue
        if in_single or in_double:
            i += 1
            continue

        if _is_word_at(i, "IF"):
            depth += 1
            i += 2
            continue
        if _is_word_at(i, "THEN") and depth == 1 and then_pos == -1:
            then_pos = i
            i += 4
            continue
        if _is_word_at(i, "ELSEIF") and depth == 1:
            i += 6
            continue
        if _is_word_at(i, "ELSE") and depth == 1:
            else_pos = i
            i += 4
            continue
        if _is_word_at(i, "ENDIF"):
            depth -= 1
            if depth == 0:
                endif_pos = i
                break
            i += 5
            continue
        i += 1

    if then_pos == -1 or else_pos == -1 or endif_pos == -1:
        return None
    condition = stripped[2:then_pos].strip()
    true_expr = stripped[then_pos + 4 : else_pos].strip()
    false_expr = stripped[else_pos + 4 : endif_pos].strip()
    if not condition or not true_expr or not false_expr:
        return None
    return condition, true_expr, false_expr


def _convert_if_then_else(expr: str, engine_vars: set[str] | None) -> str:
    """Convert IF/THEN/ELSE/ENDIF to CASE WHEN syntax."""
    parsed = _parse_if_then_else(expr)
    if parsed is None:
        return expr
    cond_raw, true_raw, false_raw = parsed
    cond = _convert_expression_impl(cond_raw, engine_vars)
    true_expr = _convert_expression_impl(true_raw, engine_vars)
    false_expr = _convert_expression_impl(false_raw, engine_vars)
    return f"(CASE WHEN {cond} THEN {true_expr} ELSE {false_expr} END)"


def _convert_expression_impl(expression: str, engine_vars: set[str] | None = None) -> str:
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

    # 0b. Rewrite control-flow helpers that previously needed LLM.
    expr = _replace_findstring_calls(expr, engine_vars)
    expr = _replace_datetimeadd_calls(expr, engine_vars)
    expr = _replace_datetimediff_calls(expr, engine_vars)
    expr = _replace_iif_calls(expr, engine_vars)
    expr = _replace_switch_calls(expr, engine_vars)
    expr = _convert_if_then_else(expr, engine_vars)

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

    # 6. Convert Alteryx ROUND(x, mult) → T-SQL ROUND(x, decimal_places).
    expr = _convert_round_calls(expr)

    # 7. Wrap date casts in CONVERT(NVARCHAR) when used in string concatenation.
    #    Must run after function renames so DATETIMETODAY() → CAST(GETDATE() AS DATE)
    #    is already in place before this step inspects the expression.
    expr = _fix_date_string_concat(expr)

    return expr


def convert_expression(expression: str, engine_vars: set[str] | None = None) -> str:
    """Public wrapper for deterministic Alteryx→T-SQL conversion."""
    return _convert_expression_impl(expression, engine_vars)
