{% macro _mtf_log(msg, macro, tag, debug=false) -%}
{#
    "Logs formatted message
    "Arg:
    "   - msg: Any   -- Log message
    "   - macro: str -- Macro name
    "   - tag: str   -- Extra marker which could help to more accurately manifest the message content
    "Raises:
    "    exceptions.raise_compiler_error
#}
    {%- if macro and tag -%}
        {% set _log_msg = macro ~ " [" ~ tag ~ "] :: " ~ msg %}
    {%- elif macro -%}
        {% set _log_msg = macro ~ " :: " ~ msg %}
    {% else %}
        {% set _log_msg = msg %}
    {%- endif -%}
    {% set _ = log(_log_msg, info=true) if debug else "" %}
{%- endmacro %}
