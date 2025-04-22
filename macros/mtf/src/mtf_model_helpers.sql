{% macro mtf_find_model(model_name) %}
{#
    "Returns DBT Ref by provided model name if any
#}
    {% set result =  {} %}
    {% if mtf_find_model_node(model_name) %}
        {{ result.update({"model": builtins.ref(model_name)}) }}
    {% endif %}
    {{ return(result.model) }}
{% endmacro %}


{% macro mtf_find_model_node(model_name) %}
{#
    "Returns DBT Model content by provided model name if any
#}
    {% set result =  {} %}
    {% set _ = _mtf_log(graph if not execute else "", "mtf_find_model_node", "graph", false) %}
    {% for dbt_node in graph.nodes.values() %}
        {% if dbt_node.name == model_name %}
            {{ result.update({"node": dbt_node}) }}
        {% endif %}
    {% endfor %}
   {{ return(result.node) }}
{% endmacro %}
