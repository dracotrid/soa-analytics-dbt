{#
    "Contains replace/override column value transformation macros:
    "   - mtf_transform_col_fn__mapped
#}


{% macro mtf_transform_col_fn__mapped(tf_column_config) -%}
{#
    "Maps a source field value using provided mapping otherwise returns a source field or default (if configured) value
    "
    "Requires: tf_column_config.source.mapped
    "Format:
    "   field: source field name
    "   mapping: relation fcn
    "   current: field name of the mapping to look up a value in
    "   target: field name of then mapping to return value of
    "   default: <any string value>, optional, the source field by default
    "Examples:
    " - tf_column_config.source.mapped:
    "       field: state
    "       mapping: state_mapping
    "       current: original_state
    "       target: internal_state
    "       [default: ABC]
#}
    {%- set source_config = tf_column_config.source.mapped  -%}
    {%- set target_config = tf_column_config.target  -%}

    {%- set source_field = source_config.field  -%}
    {%- set key_field = source_config.current_value  -%}
    {%- set value_field = source_config.target_value  -%}
    {%- if "default" in source_config -%}
        {%- if not source_config.default or source_config.default|lower == "null"  -%}
            {%- set default_value = "NULL"  -%}
        {%- else -%}
             {#- "FIXME: Add support for other data types" -#}
            {%- set default_value = "\"" ~ source_config.default|trim ~ "\""  -%}
        {%- endif -%}
    {%- else -%}
        {%- set default_value = source_field  -%}
    {%- endif -%}
    {{- _mtf_transform_column(tf__fve__value_or_default(
                "(SELECT mapping." ~ value_field ~
                    " FROM " ~ tf_ref(source_config.mapping) ~ " AS mapping"
                    " WHERE mapping." ~ key_field ~ " = " ~ source_field ~ ")",
                default_value
            ),
            tf_column_config.target
        )
    }}
{%- endmacro %}
