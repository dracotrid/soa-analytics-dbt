{#
    "Contains core column transformation macros:
    "   - mtf_transform_col_fn__constant
    "   - mtf_transform_col_fn__expression
    "   - mtf_transform_col_fn__field
    "   - mtf_transform_col_fn__macro
#}


{% macro mtf_transform_col_fn__constant(tf_column_config) -%}
{#
    "Transforms provided constant into target column with target type
    "
    "Requires: tf_column_config.source.constant
    "Constant format should be accepted by target data type i.e.
    " - STRING: any text including multiline text"
    " - DATE: 2024-01-12
    " - INT: 123
    " - NUMERIC: 12345.987654 (note, it is better to enclose numeric value in double quotes in yaml)
#}
    {%- if tf_column_config.source.constant is not none -%}
        {{- _mtf_transform_column(
            "\"" ~ tf_column_config.source.constant|string|trim|replace("\n", "\\n") ~ "\"",
            tf_column_config.target)
        }}
    {%- else -%}
        {{- _mtf_transform_column("NULL", tf_column_config.target) -}}
    {%- endif -%}
{%- endmacro %}


{% macro mtf_transform_col_fn__expression(tf_column_config) -%}
{#
    "Transforms provided expression into target column with target type
    "
    "Requires: tf_column_config.source.expression
    "Supported expressions:
    " - arithmetic operations: 1+2, field_a * field_b, etc
    " - target storage functions: CONCAT(id,'-',code), ARRAY_CONCAT([1, 2], [3, 4], [5, 6]), etc
    " - multiline SQL statements:
    "       CASE
    "           WHEN externalId IS NOT NULL THEN CONCAT(id, '--', externalId)
    "           ELSE id
    "       END
#}
    {%- set tf_source_expression = tf_column_config.source.expression.replace("\n", " ")  %}
    {{- _mtf_transform_column("(" ~ tf_source_expression ~ ")", tf_column_config.target) }}
{%- endmacro %}


{% macro mtf_transform_col_fn__field(tf_column_config, debug=false) -%}
{#
    "Transforms provided field into target column with target type
    "
    "Requires: tf_column_config.source.field
#}
    {%- set _macro_ = "mtf_transform_col_fn__field" -%}
    {%- set tf_src_col_conf = tf_column_config.source -%}
    {{- _mtf_log(tf_src_col_conf, _macro_, "tf_column_conf", debug) -}}
    {%- if "required" in tf_src_col_conf and not tf_src_col_conf.required  -%}
        {#- "TODO: Support JSON fields - map/array" -#}
         {%- set internal_json_fields = "JSON_VALUE(__mtf_internal_json_fields, '$." ~ tf_src_col_conf.field ~ "')" -%}
         {%- set file_name_parsed_fields = "JSON_VALUE(__mtf_internal_json_fields, '$.__mtf_internal_meta_json_fields." ~ tf_src_col_conf.field ~ "')" -%}
        {%- set tf_source_field = "COALESCE(" ~ file_name_parsed_fields ~ ", " ~ internal_json_fields ~ ")" -%}
    {%- else -%}
        {% if tf_src_col_conf.field.startswith('`') and tf_src_col_conf.field.endswith('`') %}
            {%- set tf_source_field = tf_src_col_conf.field -%}
        {% else %}
            {%- set tf_source_field = adapter.quote(tf_src_col_conf.field) -%}
        {% endif %}
    {%- endif -%}
    {{- _mtf_log(tf_src_col_conf.default, _macro_, "tf_column_conf.default", debug) -}}
    {{- _mtf_log("default" in tf_src_col_conf, _macro_, "default in tf_column_conf", debug) -}}
    {{- _mtf_log(tf_src_col_conf.default is defined, _macro_, "tf_column_conf.default is defined", debug) -}}
    {{- _mtf_log(tf_src_col_conf.default is none, _macro_, "tf_column_conf.default is none", debug) -}}
    {%- if tf_src_col_conf.default is defined -%}
        {#- "TODO: refactor to reuse common casting logic" -#}
        {%- if tf_src_col_conf.default is none -%}
            {%- set default_value = "NULL" -%}
        {%- else -%}
            {%- set raw_default_value = "\"" ~ tf_src_col_conf.default|string|trim|replace("\n", "\\n") ~ "\"" -%}
            {%- set target_type = tf_column_config.target.type.split("(")[0] -%}
            {%- set default_value = "CAST(" ~ raw_default_value ~ " AS " ~ target_type ~ ")" -%}
        {%- endif -%}
        {%- set tf_source_expression = tf__fve__value_or_default(tf_source_field, default_value) -%}
    {%- else -%}
        {%- set tf_source_expression = tf_source_field -%}
    {%- endif -%}
    {{- _mtf_transform_column(tf_source_expression, tf_column_config.target) }}
{%- endmacro %}


{% macro mtf_transform_col_fn__macro(tf_column_config) -%}
{#
    "Uses provided macro to create transform expression
    "
    "Requires: tf_column_config.source.macro
#}
    {%- set expression_factory_fn = context.get(tf_column_config.source.macro) -%}
    {%- if expression_factory_fn -%}
        {#- "TODO: Evaluate params" -#}
        {%- set tf_source_expression = expression_factory_fn(*tf_column_config.source.params).replace("\n", " ") -%}
        {{- _mtf_transform_column("(" ~ tf_source_expression ~ ")", tf_column_config.target) }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("[" ~ tf_column_config.source.macro ~ "] expression factory macro not found") }}
    {%- endif -%}
{%- endmacro %}
