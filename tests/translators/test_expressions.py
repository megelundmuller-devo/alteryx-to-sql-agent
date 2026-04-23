"""Tests for src/translators/expressions.py — deterministic Alteryx→T-SQL expression converter."""


from translators.expressions import convert_expression, needs_llm_translation


class TestNeedsLlm:
    def test_if_then_needs_llm(self):
        assert needs_llm_translation("IF [x] > 0 THEN 1 ELSE 0 ENDIF") is True

    def test_regex_needs_llm(self):
        assert needs_llm_translation('REGEX_Match([Email], ".*@.*")') is True

    def test_iif_needs_llm(self):
        assert needs_llm_translation("IIF([x] > 0, 1, 0)") is True

    def test_simple_comparison_no_llm(self):
        assert needs_llm_translation("[Status] = 'Active'") is False

    def test_contains_no_llm(self):
        assert needs_llm_translation('CONTAINS([Name], "test")') is False

    def test_isnull_no_llm(self):
        assert needs_llm_translation("ISNULL([Col])") is False

    def test_arithmetic_no_llm(self):
        assert needs_llm_translation("[Amount] * 1.2 + [Tax]") is False

    def test_datetimeadd_needs_llm(self):
        assert needs_llm_translation("DATETIMEADD('days', 1, [Date])") is True


class TestConvertExpression:
    def test_double_quotes_to_single(self):
        result = convert_expression('"hello"')
        assert result == "'hello'"

    def test_escaped_single_quote_in_string(self):
        # "it's" → 'it''s'
        result = convert_expression('"it\'s"')
        assert "it''s" in result

    def test_null_literal(self):
        assert convert_expression("[Null]") == "NULL"
        assert convert_expression("[NULL]") == "NULL"

    def test_true_false_literals(self):
        assert convert_expression("True") == "1"
        assert convert_expression("False") == "0"
        assert convert_expression("true AND false") == "1 AND 0"

    def test_contains_to_like(self):
        result = convert_expression('CONTAINS([Name], "test")')
        assert "[Name] LIKE '%test%'" in result

    def test_startswith_to_like(self):
        result = convert_expression('STARTSWITH([Prefix], "AB")')
        assert "[Prefix] LIKE 'AB%'" in result

    def test_isnull_to_is_null(self):
        result = convert_expression("ISNULL([Col])")
        assert "[Col] IS NULL" in result

    def test_isnotnull_to_is_not_null(self):
        result = convert_expression("ISNOTNULL([Col])")
        assert "[Col] IS NOT NULL" in result

    def test_trim_to_ltrim_rtrim(self):
        result = convert_expression("TRIM([Name])")
        assert "LTRIM(RTRIM([Name]))" in result

    def test_length_to_len(self):
        result = convert_expression("LENGTH([Name])")
        assert "LEN([Name])" in result

    def test_uppercase_to_upper(self):
        result = convert_expression("UPPERCASE([Name])")
        assert "UPPER([Name])" in result

    def test_lowercase_to_lower(self):
        result = convert_expression("LOWERCASE([Name])")
        assert "LOWER([Name])" in result

    def test_datetimenow(self):
        result = convert_expression("DATETIMENOW()")
        assert "GETDATE()" in result

    def test_datetimetoday(self):
        result = convert_expression("DATETIMETODAY()")
        assert "CAST(GETDATE() AS DATE)" in result

    def test_tonumber(self):
        result = convert_expression("TONUMBER([Amount])")
        assert "TRY_CAST([Amount] AS FLOAT)" in result

    def test_tostring(self):
        result = convert_expression("TOSTRING([Id])")
        assert "CAST([Id] AS NVARCHAR(MAX))" in result

    def test_passthrough_arithmetic(self):
        expr = "[Price] * [Qty] + [Tax]"
        result = convert_expression(expr)
        assert result == expr  # unchanged

    def test_passthrough_column_refs(self):
        expr = "[Col1] = [Col2]"
        result = convert_expression(expr)
        assert result == expr

    def test_todate(self):
        result = convert_expression("TODATE([timestamp])")
        assert "CAST([timestamp] AS DATE)" in result

    def test_isinteger(self):
        result = convert_expression("ISINTEGER([ACCOUNT])")
        assert "TRY_CAST([ACCOUNT] AS INT) IS NOT NULL" in result

    def test_titlecase(self):
        result = convert_expression("TITLECASE([name])")
        assert "UPPER(LEFT([name], 1))" in result
        assert "LOWER(SUBSTRING([name], 2, LEN([name])))" in result

    def test_datetimefirstofmonth(self):
        result = convert_expression("DATETIMEFIRSTOFMONTH()")
        assert "DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)" in result

    def test_engine_var_replaced(self):
        evars: set[str] = set()
        result = convert_expression("[Engine.WorkflowFileName]", evars)
        assert result == "@WorkflowFileName"
        assert "WorkflowFileName" in evars

    def test_engine_var_directory(self):
        evars: set[str] = set()
        result = convert_expression(
            "'prefix_' + [Engine.WorkflowDirectory] + '_suffix'", evars
        )
        assert "@WorkflowDirectory" in result
        assert "WorkflowDirectory" in evars

    def test_engine_var_no_collection_when_none(self):
        # When engine_vars=None, replacement still happens but nothing is collected
        result = convert_expression("[Engine.WorkflowFileName]", None)
        assert result == "@WorkflowFileName"

    def test_round_hundredths(self):
        result = convert_expression("ROUND([Amount], 0.01)")
        assert result == "ROUND([Amount], 2)"

    def test_round_thousandths(self):
        result = convert_expression("ROUND([Amount], 0.001)")
        assert result == "ROUND([Amount], 3)"

    def test_round_tenths(self):
        result = convert_expression("ROUND([Amount], 0.1)")
        assert result == "ROUND([Amount], 1)"

    def test_round_ones(self):
        result = convert_expression("ROUND([Amount], 1)")
        assert result == "ROUND([Amount], 0)"

    def test_round_tens(self):
        result = convert_expression("ROUND([Amount], 10)")
        assert result == "ROUND([Amount], -1)"

    def test_round_nested_expression(self):
        result = convert_expression("ROUND([Price] * [Qty], 0.01)")
        assert result == "ROUND([Price] * [Qty], 2)"

    def test_round_non_literal_second_arg_passthrough(self):
        # If the second arg is a column ref, we can't convert — leave unchanged.
        result = convert_expression("ROUND([Amount], [Precision])")
        assert result == "ROUND([Amount], [Precision])"
