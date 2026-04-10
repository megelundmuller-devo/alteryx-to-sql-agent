# BI_Aggregate Daily Simple_LDB-01

Source: `BI_Aggregate Daily Simple_LDB-01.yxmd`  
Tools: 51 | Chunks: 42 | CTEs: 51 | Stubs: 2

## Overview

Excellent. Here is the documentation for the Alteryx workflow and its SQL conversion.

***

### Workflow: BI_Aggregate Daily Simple_LDB-01

This workflow aggregates daily user activity data from the StudyBox platform. It enriches this raw data by joining it with customer, publication, and accounting information from multiple databases. The final output is a consolidated daily summary table designed for business intelligence reporting.

---

### Data Processing Steps

The SQL script is organized into Common Table Expressions (CTEs), where each CTE corresponds to one or more tools from the original Alteryx workflow.

#### 1. Data Ingestion

The workflow begins by loading data from six different sources:

*   **`cte_db_file_input_2`**: Loads all records from the `Publications` table in the `CooperC5` database.
*   **`cte_db_file_input_29`**: Loads all records from the `CUSTTABLE` table in the `CooperC5` database.
*   **`cte_db_file_input_50`**: Loads all records from the `accounting` table in the `CooperBilling` database.
*   **`cte_db_file_input_71`**: Loads user data from the `StudyBoxUser...` table in the `CooperData` database.
*   **`cte_text_input_14` & `cte_text_input_66`**: Load static, manually entered data defined within the workflow. This is often used for mapping or lookup tables.

#### 2. Data Preparation and Transformation

The raw data is then cleaned, transformed, and standardized through a series of steps:

*   **Filtering**: Several streams are filtered early in the process.
    *   **`cte_filter_30`**: Isolates records where the `DATASET` field is `"DAT"`.
    *   **`cte_filter_45`**: Ensures data integrity by keeping only records where the `ACCOUNT` field is an integer.
    *   **`cte_filter_17_to_select_18`**: Removes records identified as demo accounts (`demo_url != 1`).
*   **Column Derivation (Formulas)**: New columns are created to add business context.
    *   **`cte_formula_38`**: Calculates a `SchoolYear` based on the `timestamp` (school years typically start in August).
    *   **`cte_formula_87`**: Translates a numeric `mainUserGroup` code into a descriptive user type (e.g., "Studerende").
    *   **`cte_formula_43`**: Creates a numeric `CompanyID` from the `ACCOUNT` field.
    *   **`cte_formula_15`**: Extracts an 8-character product number (`ops_varenummer`) from the `isbn` field.
    *   **`cte_formula_12`**: Standardizes `UserSubject` values by consolidating similar categories (e.g., mapping `"eud/eux/kuu"` to `"eud/eux"`).
    *   **`cte_formula_84`, `cte_formula_81`, `cte_formula_53`**: Apply a series of rules to clean and standardize `Publisher` names based on `customerId` and `myAccountId`.
*   **Column Management (`select`)**: Throughout the workflow, `select` steps (`cte_select_36`, `cte_select_6`, etc.) are used to rename columns, change data types, and remove unnecessary fields to optimize processing.
*   **Record ID Generation**: Steps like `cte_record_id_20` add a unique row identifier to data streams, often in preparation for joins or debugging.

#### 3. Data Merging and Aggregation

Once prepared, the separate data streams are combined and aggregated.

*   **Unions**: Several `union` steps (`cte_union_67`, `cte_union_79`, etc.) stack different data streams together. Some of these are combined with a `unique` step to remove duplicate records after the union.
*   **Joins**: Multiple `join` steps (`cte_join_4`, `cte_join_72`, etc.) merge the different data streams (users, customers, publications, accounting) based on common keys.
*   **Summarization**: `summarize` steps (`cte_summarize_39`, `cte_summarize_74`) perform the core aggregation, grouping records by various dimensions (like publisher, user type, and date) and calculating metrics.
*   **Deduplication**: `unique` steps (`cte_unique_77`, `cte_union_65_to_unique_47`) are used to remove fully duplicate rows from the dataset.

#### 4. Final Output

The processed data is split into two final outputs:

1.  **Main Output Table**: The primary data stream goes through a final series of transformations (`cte_union_78_to_db_file_output_49`), including:
    *   Calculating a `Publisher_Final` value.
    *   Adding workflow metadata (`WorkflowName`, `WorkflowPath`).
    *   Passing through a `Cleanse` macro (see Warnings below).
    *   The result is written to the `Aggregated_Daily_simple` table in the `CooperData` database.
2.  **Filtered Output**: A secondary output stream (`cte_filter_70`) is created by filtering out records where the `Publisher` is `"Other StudyBox"`. This branch does not write to a database and may have been used for debugging or a separate analysis path in Alteryx.

---

### Manual Review Items and Warnings

**Please pay close attention to the following items, as they require manual intervention.**

*   **Macro Stubs (Critical)**:
    *   Tools `31` and `52` in the original workflow were Alteryx macros named `Cleanse.yxmc`. The conversion tool could not expand this logic and has created placeholder CTEs (`cte_macro_31` and `cte_macro_52`).
    *   **Action Required**: You must manually inspect the `Cleanse.yxmc` macro and implement its data cleansing logic in SQL to ensure the final output is correct. This logic is currently missing from the generated script.

*   **Table Write Mode**:
    *   The final output tool (`db_file_output_49`) was configured to **Overwrite** the target table `Aggregated_Daily_simple`.
    *   **Action Required**: The generated SQL script ends with a `SELECT` statement. To match the original workflow's behavior, replace the final `SELECT * FROM [cte_macro_52]` line with an `INSERT` statement, for example:
      ```sql
      -- Replace final SELECT with this block
      -- TRUNCATE TABLE CooperData.dbo.Aggregated_Daily_simple; -- If a full overwrite is needed
      INSERT INTO CooperData.dbo.Aggregated_Daily_simple
      SELECT * FROM [cte_macro_52];
      ```

## Data Flow

**Sources**

- `db_file_input` (tool 2) — aka:CooperC5
Query=select * 
from Publications
- `text_input` (tool 14)
- `db_file_input` (tool 29) — aka:CooperC5
Query=select * 
from CUSTTABLE
- `db_file_input` (tool 50) — aka:CooperBilling
Query=select * 
from accounting
- `text_input` (tool 66)
- `db_file_input` (tool 71) — aka:CooperData
Query=select * 
from StudyBoxUser...

**Processing** (topological order)

- `cte_db_file_input_2`: `db_file_input` _aka:CooperC5
Query=select * 
from Publications_
- `cte_text_input_14`: `text_input`
- `cte_db_file_input_29`: `db_file_input` _aka:CooperC5
Query=select * 
from CUSTTABLE_
- `cte_db_file_input_50`: `db_file_input` _aka:CooperBilling
Query=select * 
from accounting_
- `cte_text_input_66`: `text_input`
- `cte_db_file_input_71`: `db_file_input` _aka:CooperData
Query=select * 
from StudyBoxUser..._
- `cte_select_36`: `select`  reads: `cte_db_file_input_2`
- `cte_select_6`: `select`  reads: `cte_text_input_14`
- `cte_filter_30`: `filter` _[DATASET] = "DAT"_  reads: `cte_db_file_input_29`
- `cte_select_86`: `select`  reads: `cte_db_file_input_50`
- `cte_select_89`: `select`  reads: `cte_db_file_input_71`
- `cte_macro_31`: `macro`  reads: `cte_filter_30`
- `cte_formula_38`: `formula` _SchoolYear = IF DateTimeMonth([timestamp]) > 7 THEN
ToString(DateTimeYear([times..._  reads: `cte_select_86`
- `cte_filter_45`: `filter` _IsInteger([ACCOUNT])_  reads: `cte_macro_31`
- `cte_formula_87`: `formula` _MainUserType = IF [mainUserGroup] = "3" THEN "Studerende"
ELSEIF [mainUserGroup]..._  reads: `cte_formula_38`
- `cte_formula_43`: `formula` _CompanyID = ToNumber([ACCOUNT])_  reads: `cte_filter_45`
- `cte_formula_15`: `formula` _ops_varenummer = If [isbn] = "MISSING" THEN [isbn]
Else right([isbn], 8)
ENDIF_  reads: `cte_formula_87`
- `cte_select_44`: `select`  reads: `cte_formula_43`
- `cte_record_id_20`: `record_id`  reads: `cte_formula_15`
- `cte_union_67`: `union`  reads: `cte_select_44`, `cte_text_input_66`
- `cte_formula_12`: `formula` _UserSubject = iF [UserSubject] IN ("eud/eux", "eud/eux/kuu") THEN "eud/eux"
else..._  reads: `cte_record_id_20`
- `cte_join_4`: `join`  reads: `cte_select_6`, `cte_formula_12`
- `cte_filter_17_to_select_18`: `filter` _[demo_url] != 1_ → `select`  reads: `cte_join_4`
- `cte_union_16_to_filter_69`: `union` → `unique` → `filter` _[myAccountId] in ("STUDYBOXMYACCOUNT","MUNKSGAARDMYACCOUNT")_  reads: `cte_join_4`, `cte_filter_17_to_select_18`
- `cte_filter_80`: `filter` _[myAccountId] = "SYSTIMEMYACCOUNT"_  reads: `cte_union_16_to_filter_69`
- `cte_formula_84`: `formula` _Publisher = TitleCase([customerId])_  reads: `cte_union_16_to_filter_69`
- `cte_formula_53`: `formula` _Publisher = If [myAccountId] = "STUDYBOXMYACCOUNT" AND [customerId] = "Munksgaar..._  reads: `cte_union_16_to_filter_69`
- `cte_summarize_39`: `summarize`  reads: `cte_filter_80`
- `cte_join_72`: `join`  reads: `cte_formula_84`, `cte_select_89`
- `cte_filter_70`: `filter` _[Publisher] != "Other StudyBox"_  reads: `cte_formula_53`
- `cte_formula_81`: `formula` _Publisher = TitleCase([customerId])_  reads: `cte_summarize_39`
- `cte_union_79`: `union`  reads: `cte_join_72`
- `cte_record_id_46`: `record_id`  reads: `cte_formula_81`
- `cte_summarize_74`: `summarize`  reads: `cte_union_79`
- `cte_join_3`: `join`  reads: `cte_select_36`, `cte_record_id_46`
- `cte_record_id_76`: `record_id`  reads: `cte_summarize_74`
- `cte_select_51`: `select`  reads: `cte_join_3`
- `cte_join_75`: `join`  reads: `cte_select_36`, `cte_record_id_76`
- `cte_join_32`: `join`  reads: `cte_select_51`, `cte_union_67`
- `cte_unique_77`: `unique`  reads: `cte_join_75`
- `cte_union_65_to_unique_47`: `union` → `unique`  reads: `cte_join_32`
- `cte_union_78_to_db_file_output_49`: `union` → `formula` _Publisher_Final = IF [PublisherERP] = "Dansklærerforeningen & Systime" AND [Publ..._ → `formula` _WorkflowName = [Engine.WorkflowFileName]
WorkflowPath = [Engine.WorkflowDirector..._ → `select` → `macro` → `db_file_output` _aka:CooperData
Query=Aggregated_Daily_simple_  reads: `cte_union_65_to_unique_47`, `cte_unique_77`

**Sinks**

- `db_file_output` (tool 49) — aka:CooperData
Query=Aggregated_Daily_simple
- `filter` (tool 70) — [Publisher] != "Other StudyBox"

## CTEs

- `cte_db_file_input_2`
- `cte_text_input_14`
- `cte_db_file_input_29`
- `cte_db_file_input_50`
- `cte_text_input_66`
- `cte_db_file_input_71`
- `cte_select_36`
- `cte_select_6`
- `cte_filter_30`
- `cte_select_86`
- `cte_select_89`
- `cte_macro_31` ⚠ stub
- `cte_formula_38`
- `cte_filter_45`
- `cte_formula_87`
- `cte_formula_43`
- `cte_formula_15`
- `cte_select_44`
- `cte_record_id_20`
- `cte_union_67`
- `cte_formula_12`
- `cte_join_4`
- `cte_filter_17`
- `cte_filter_17_to_select_18`
- `cte_union_16`
- `cte_unique_19`
- `cte_union_16_to_filter_69`
- `cte_filter_80`
- `cte_formula_84`
- `cte_formula_53`
- `cte_summarize_39`
- `cte_join_72`
- `cte_filter_70`
- `cte_formula_81`
- `cte_union_79`
- `cte_record_id_46`
- `cte_summarize_74`
- `cte_join_3`
- `cte_record_id_76`
- `cte_select_51`
- `cte_join_75`
- `cte_join_32`
- `cte_unique_77`
- `cte_union_65`
- `cte_union_65_to_unique_47`
- `cte_union_78`
- `cte_formula_85`
- `cte_formula_24`
- `cte_select_48`
- `cte_macro_52` ⚠ stub
- `cte_union_78_to_db_file_output_49`

## Warnings

- Tool 31 (macro): references 'Cleanse.yxmc'. Macro expansion is not yet implemented — stub CTE emitted. Review manually.
- Tool 52 (macro): references 'Cleanse.yxmc'. Macro expansion is not yet implemented — stub CTE emitted. Review manually.
- Tool 49 (db_file_output): writes to [Aggregated_Daily_simple] (mode: Overwrite). Replace the trailing SELECT in the final script with: INSERT INTO [Aggregated_Daily_simple] SELECT * FROM [cte_macro_52]
