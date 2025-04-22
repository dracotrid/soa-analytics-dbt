{% macro _mtf_load_macro(macro_name, error_msg, debug=false) %}
{#
    "Returns macro function (callable) if any otherwise raises an error
    "Arg:
    "   - msg: Any   -- Log message
    "   - macro: str -- Macro name
    "   - tag: str   -- Extra marker which could help to more accurately manifest the message content
    "Returns:
    "   callable -- macro function
    "Raises:
    "    exceptions.compiler_error -- macro not found
#}
    {% set _macro_ = "_mtf_load_macro" %}
    {{ _mtf_log(macro_name, _macro_, "macro_name", debug) }}

    {% set macro_callable = context.get(macro_name) %}
    {% if macro_callable %}
        {{ return(macro_callable) }}
    {% else %}
        {% set error_msg = error_msg if error_msg else "macro not found" %}
        {{ exceptions.raise_compiler_error("[" ~ macro_name ~ "]: " ~ error_msg) }}
    {% endif %}
{% endmacro %}
