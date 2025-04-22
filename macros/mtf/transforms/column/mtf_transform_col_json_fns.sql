{#
    "Contains JSON column transformation macros:
    "   - mtf_transform_col_fn__json_field
    "   - mtf_transform_col_fn__json_array_field
#}


{% macro mtf_transform_col_fn__json_field(tf_column_config) -%}
{#
    "Transforms value extracted from specified JSON column using relative json path into target column
    "
    "Requires: tf_column_config.source.json_field
    "
    "Format in Model column yaml:
    "tf_config:
    "   json_field: {source_field_name}.{path.to.value}
    "
    "Examples:
    " - workerDates.originalHireDate
    " - person.legalName.familyName1
    " - businessCommunication.emails[0].emailUri
    " - workAssignments[0].jobTitle
#}
    {%- if tf_column_config.target.type|lower == "json" -%}
        {%- set source_expression = tf_column_config.source.json_field  -%}
    {%- else -%}
        {%- set fq_jpath = tf_column_config.source.json_field.split(".")  -%}
        {%- set column_name = fq_jpath[0].split("[")[0]  -%}
        {%- set json_root = fq_jpath[0].replace(column_name, "$")  -%}
        {%- set source_expression = "JSON_VALUE(" ~ column_name ~ ", '" ~ json_root ~ "." ~ ".".join(fq_jpath[1:]) ~ "')"  -%}
    {%- endif -%}
    {{- _mtf_transform_column(source_expression, tf_column_config.target) -}}
{%- endmacro %}


{% macro mtf_transform_col_fn__json_array_field(tf_column_config) -%}
{#
    "Transforms an embedded value extracted from optionally sorted JSON Array column first entry into target column
    "
    "Requires: tf_column_config.source.json_array_field
    "
    "Format in Model column yaml:
    "tf_config:
    "   json_array_field:
    "       array_field: str -- json array field name
    "       array_alias: str -- any SQL compatible string, default is 'af'
    "       value_path: str  -- relative json path to value (i.e. path.to.value)
    "       extractor: str   -- name of an extractor macro, default is 'mtf__extract__array_field_value'
    "       priority: list   -- defines a way to identify the most relevant entry, default is natural order
    "           value_path: str -- relative json path to sort value (i.e. path.to.sort.value)
    "           type: str       -- data type to cast the value to
    "           sort: str       -- ASK or DESC, default is ASK
    "           safe_cast: bool -- whether to use SAFE_CAST or CAST over the value, default is true
    "
    "Examples:
    "  1. == Fetch First (similar to json_field: json_array_column_name[0].path.to.json.value) ==
    "   tf_config:
    "       json_array_field:
    "           array_field: json_array_column_name
    "           value_path: path.to.json.value
    "
    "  2. == Fetch First ordered ascending by path.to.sort.field ==
    "   tf_config:
    "       json_array_field:
    "           array_field: json_array_column_name
    "           value_path: path.to.json.value
    "           priority:
    "               value_path: path.to.sort.value
    "               type: INTEGER
    "
    "  3. == Fetch First ordered descending by path.to.sort.field ==
    "   tf_config:
    "       json_array_field:
    "           array_field: json_array_column_name
    "           value_path: path.to.json.value
    "           priority:
    "               value_path: path.to.sort.value
    "               type: DATE
    "               sort: DESC
    "               safe_cast: false
    "
    "  4. == Fetch First using a custom extractor and alias ==
    "   tf_config:
    "       json_array_field:
    "           array_field: json_array_column_name
    "           array_alias: wa
    "           value_path: path.to.json.value
    "           extractor: adp__extract__department
    "           priority:
    "               value_path: path.to.sort.value
    "               type: DATE
    "               sort: DESC
#}
    {#- "TODO: Add validation for the input parameters" -#}
    {%- set json_array_field_conf = tf_column_config.source.json_array_field -%}
    {%- set array_field = json_array_field_conf.array_field  -%}
    {%- set array_alias = json_array_field_conf.get("array_alias", "af") -%}
    {%- set value_path = json_array_field_conf.value_path  -%}
    {%- set priority = json_array_field_conf.priority -%}
    {#- "TODO: Add type reusable type resolver across code base" -#}
    {%- set target_type = tf_column_config.target.type|lower -%}
    {%- set extractor_name = json_array_field_conf.get("extractor", "tf__fve__array_field_value") -%}

    {%- set source_expression =
            _mtf_load_macro(extractor_name)(array_field, array_alias, value_path, priority, target_type).replace("\n", " ") -%}
    {#- "TODO: Add default support (review all the column level transformations where it is reasonable to support default" -#}
    {{- _mtf_transform_column("(" ~ source_expression ~ ")", tf_column_config.target) -}}
{%- endmacro %}
