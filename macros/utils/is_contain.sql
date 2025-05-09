{% macro is_contain(key_field, source_name, check_key_field, source_type) %}
    {#
        "Check if field check_key_field from table check_table contain value key_field
    #}

    {% if source_type == 'model' %}
        {%- set check_source = tf_ref(source_name) -%}
    {% elif source_type == 'source' -%}
        {%- set check_source = tf_source(source_name) -%}
    {% else -%}
        {%- set _macro_ = 'is_contain' -%}
        {{ exceptions.raise_compiler_error(_macro_ ~ ". Source name: " ~ source_name ~ ". Wrong source type: " ~ source_type) }}
    {%- endif %}

    CASE
        WHEN {{ key_field }} IN (SELECT {{ check_key_field }} FROM {{ check_source }})
            THEN TRUE
        ELSE FALSE
    END
{% endmacro %}
