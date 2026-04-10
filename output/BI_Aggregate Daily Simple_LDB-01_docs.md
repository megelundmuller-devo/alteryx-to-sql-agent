# BI_Aggregate Daily Simple_LDB-01

Source: `BI_Aggregate Daily Simple_LDB-01.yxmd`  
Tools: 51 | Chunks: 42 | CTEs: 51 | Stubs: 8

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
- `cte_formula_38` ⚠ stub
- `cte_filter_45`
- `cte_formula_87` ⚠ stub
- `cte_formula_43`
- `cte_formula_15` ⚠ stub
- `cte_select_44`
- `cte_record_id_20`
- `cte_union_67`
- `cte_formula_12` ⚠ stub
- `cte_join_4`
- `cte_filter_17`
- `cte_filter_17_to_select_18`
- `cte_union_16`
- `cte_unique_19`
- `cte_union_16_to_filter_69`
- `cte_filter_80`
- `cte_formula_84`
- `cte_formula_53` ⚠ stub
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
- `cte_formula_85` ⚠ stub
- `cte_formula_24`
- `cte_select_48`
- `cte_macro_52` ⚠ stub
- `cte_union_78_to_db_file_output_49`

## Warnings

- Tool 31 (macro): references 'Cleanse.yxmc'. Macro expansion is not yet implemented — stub CTE emitted. Review manually.
- Tool 38 (formula): 1 field(s) require LLM translation — stub emitted:
  [SchoolYear] = 'IF DateTimeMonth([timestamp]) > 7 THEN\nToString(DateTimeYear([timestamp])) + "/" + toString(toNumber(DateTimeYear([timestamp])+1))\nELSEIF \nDateTimeMonth([timestamp]) < 8 THEN\nToString(ToNumber(DateTimeYear([timestamp])-1)) + "/" + toString(DateTimeYear([timestamp]))\nELSE "Error"\nENDIF'
- Tool 87 (formula): 1 field(s) require LLM translation — stub emitted:
  [MainUserType] = 'IF [mainUserGroup] = "3" THEN "Studerende"\nELSEIF [mainUserGroup] = "21" THEN "Valideret lærer"\nELSEIF [mainUserGroup] = "2" THEN "Lærere"\nELSEIF [mainUserGroup] = "4" THEN "Private"\nELSE "NA"\nENDIF'
- Tool 15 (formula): 1 field(s) require LLM translation — stub emitted:
  [ops_varenummer] = 'If [isbn] = "MISSING" THEN [isbn]\nElse right([isbn], 8)\nENDIF'
- Tool 12 (formula): 1 field(s) require LLM translation — stub emitted:
  [UserSubject] = 'iF [UserSubject] IN ("eud/eux", "eud/eux/kuu") THEN "eud/eux"\nelseif [UserSubject] in ("Andet","N/A","Not set") THEN "Andet"\nELSEif [UserSubject] in ("avu/fvu/obu/fgu") THEN "avu/fvu/du/fgu"\nelseif IsEmpty([UserSubject]) THEN "Andet"\nELSE [UserSubject]\nENDIF'
- Tool 53 (formula): 1 field(s) require LLM translation — stub emitted:
  [Publisher] = 'If [myAccountId] = "STUDYBOXMYACCOUNT" AND [customerId] = "Munksgaard" THEN "StudyBox"\nELSEIF [myAccountId] = "STUDYBOXMYACCOUNT" AND [customerId] = "Hans Reitzel" THEN "StudyBox"\nELSEIF [myAccountId] = "STUDYBOXMYACCOUNT" THEN "Other StudyBox"\nELSE [customerId]\nENDIF'
- Tool 85 (formula): 1 field(s) require LLM translation — stub emitted:
  [Publisher_Final] = 'IF [PublisherERP] = "Dansklærerforeningen & Systime" AND [Publisher] = "Systime" THEN "Dansklærerforeningen & Systime"\nELSEIF [PublisherERP] = "Dansklærerforeningens Forlag" AND [Publisher] = "Dlf" THEN "Dansklærerforeningens Forlag"\nELSEIF [PublisherERP] = "DlfF Forlag" AND [Publisher] = "Systime" THEN "Systime"\nELSEIF [PublisherERP] = "FNAE" AND [Publisher] = "Systime" THEN "Systime"\nELSEIF [PublisherERP] = "Forlaget Columbus" AND [Publisher] = "Columbus" THEN "Forlaget Columbus"\nELSEIF [PublisherERP] = "GU Gym" AND [Publisher] = "Gyldendal" THEN "Gyldendal"\nELSEIF [PublisherERP] = "GU Sys" AND [Publisher] = "Systime" THEN "Systime"\nELSEIF [PublisherERP] = "Gyldendal" AND [Publisher] = "Gyldendal-voksen" THEN "Gyldendal"\nELSEIF [PublisherERP] = "Hans Reitzel" AND [Publisher] = "Munksgaard" THEN "Munksgaard"\nELSEIF [PublisherERP] = "Hans Reitzel" AND [Publisher] = "Systime" THEN "Systime"\nELSEIF [PublisherERP] = "Gyldendal" AND [Publisher] = "Systime" THEN "Systime"\nELSEIF [PublisherERP] = "Systime" AND [Publisher] = "Systimesolutions" THEN "Systimesolutions"\nELSEIF [PublisherERP] = "Systime/Hans Reitzel" AND [Publisher] = "Hansreitzel" THEN "Hans Reitzel"\nELSEIF [PublisherERP] = "Systime/Hans Reitzel" AND [Publisher] = "Systime" THEN "Systime"\nELSEIF [PublisherERP] = "GU Gym" THEN "Gyldendal"\nELSEIF [PublisherERP] = "GU Sys" THEN "Systime"\nELSEIF [PublisherERP] = "VIA-Systime" THEN "Systime"\nELSEIF [PublisherERP] = "Kurlund" THEN "Systime"\nELSEIF [PublisherERP] = "DlfF Forlag" THEN "Dansklærerforeningen & Systime"\nELSE [PublisherERP]\nENDIF'
- Tool 52 (macro): references 'Cleanse.yxmc'. Macro expansion is not yet implemented — stub CTE emitted. Review manually.
- Tool 49 (db_file_output): writes to [Aggregated_Daily_simple] (mode: Overwrite). Replace the trailing SELECT in the final script with: INSERT INTO [Aggregated_Daily_simple] SELECT * FROM [cte_macro_52]
