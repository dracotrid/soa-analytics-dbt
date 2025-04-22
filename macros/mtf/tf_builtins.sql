{#
    "Contains handy client-level transform builtins macros:
    "   - tf_source
    "   - tf_ref
#}


{% macro tf_source(source_name) %}
{#
    "Loads specified DBT Source
    "Args:
    "   source_name: str, format <source_namespace.source_table_name>
#}
    {% set source_fqn = source_name.split('.') %}
    {% if source_fqn|length > 1 %}
        {% set dbt_source_schema = source_fqn[0] %}
        {% set dbt_source_name = source_fqn[1] %}
    {% else %}
        {% set source_fqn = source_name.split('__') %}
        {% set dbt_source_schema = source_fqn[0] %}
        {% set dbt_source_name = source_fqn[1:]|join("__") %}
    {% endif %}
    {{ return(builtins.source(dbt_source_schema, dbt_source_name)) }}
{% endmacro %}


{% macro tf_ref(ref_name) %}
{#
    "Wraps ref builtin changing its name
    "Args:
    "   ref_name: str
#}
    {{ return(builtins.ref(ref_name)) }}
{% endmacro %}


{% macro tf_depends_on(ref_names) -%}
{#
    "Binds provided models as dependencies
    "Args:
    "   ref_names: list[str]
#}
    {%- for ref_name in ref_names %}
-- {{ tf_ref(ref_name) }}
    {%- endfor %}
{%- endmacro %}


{% macro tf_bind_dependencies(ref_names) -%}
{#
    "Binds provided models as upstream dependencies
    "Args:
    "   ref_names: list[str]
#}
    {{ tf_depends_on(ref_names) }}
{%- endmacro %}
