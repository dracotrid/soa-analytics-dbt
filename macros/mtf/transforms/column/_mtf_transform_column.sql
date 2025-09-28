{% macro _mtf_transform_column(tf_source_expression, tf_column_target_config, debug=false) -%}
{#
    "Transforms provided generic sql expression into target column with target type
    "
    "Protected Level Func: The function might be modified to enhance its functionality
#}
    {%- set _macro_ = "_mtf_transform_column" -%}
    {%- set _ = log(_macro_ ~ " :: tf_source_expression: " ~ tf_source_expression, info=true) if debug else "" -%}
    {%- set _ = log(_macro_ ~ " :: tf_column_target_config: " ~ tf_column_target_config, info=true) if debug else "" -%}

    {%- set target = tf_column_target_config -%}

    {%- set _target_type = { "value": tf_column_target_config.type.split("(")[0] } -%}

    {%- set _tf_source_expression = tf_source_expression -%}
    {%- set target_type_value_resolver = _mtf_load_macro("mtf__extension__config__target_type_value_resolver", raise_error=false) -%}
    {%- if target_type_value_resolver: -%}
        {%- set _tf_source_expression = target_type_value_resolver(tf_column_target_config.logic_type, tf_source_expression) -%}
    {%- endif -%}

    {%- set base_type_extractor = _mtf_load_macro("mtf__extension__config__base_type_extractor", raise_error=false) -%}
    {%- if base_type_extractor -%}
        {%- set _ = _target_type.update({"value": base_type_extractor(_target_type.value)}) -%}
    {%- endif -%}

    {%- set target_type = _target_type.value -%}
    {#- "TODO: Add flexible casting configuration options " -#}
    {%- if target_type == "DATE" -%}
        {%- set src_expression = {"value": "SAFE_CAST(" ~ _tf_source_expression ~ " AS " ~ target_type ~ ")"} -%}
    {%- else -%}
        {%- set src_expression = {"value": "CAST(" ~ _tf_source_expression ~ " AS " ~ target_type ~ ")"} -%}
    {%- endif -%}
    {%- if target_type == "STRING" -%}
        {%- set _ = src_expression.update({"value": "TRIM(" ~ src_expression.value ~ ")"}) -%}
    {%- endif -%}
    {{- src_expression.value }} AS {{ adapter.quote(target.field) -}}
{%- endmacro %}
