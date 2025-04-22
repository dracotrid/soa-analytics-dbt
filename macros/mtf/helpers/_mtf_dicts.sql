{% macro _mtf_dict_set_value_if_undefined(dict, node_name, node_value, debug=false) %}
{#
    "Updates a node of provided dict with provided value if the node is not set
    "Arg:
    "   - dict: dict      -- any dict
    "   - node_name: str  -- dict node name to update
    "   - node_value: Any -- dict node value to set
    "Returns:
    "   dict -- updated dict
    "Raises:
    "    exceptions.compiler_error -- node not found
#}
    {% set _macro_ = "_mtf_set_value_if_undefined" %}
    {{ _mtf_log(dict, _macro_, "dict", debug) }}
    {{ _mtf_log(node_name, _macro_, "node_name", debug) }}

    {% if not dict.get(node_name) %}
        {{ dict.update({node_name: node_value}) }}
    {% endif %}

    {{ return(dict) }}
{% endmacro %}
