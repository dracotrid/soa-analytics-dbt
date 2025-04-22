{% macro mtf_find_source(source_name) %}
{#
    "Returns DBT Source by provided source name if any
#}
    {% set source_fqn = source_name.split('.') %}
    {% set dbt_source_name = source_fqn[0] %}
    {% set dbt_source_id = source_fqn[1] %}
    {% set result =  {} %}
    {% for dbt_source in graph.sources.values() %}
         {% if dbt_source.source_name == dbt_source_name and dbt_source.name == dbt_source_id %}
            {{ result.update({"source": builtins.source(dbt_source_name, dbt_source_id)}) }}
         {% endif %}
    {% endfor %}
   {{ return(result.source) }}
{% endmacro %}
