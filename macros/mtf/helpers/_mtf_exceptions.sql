{% macro _mtf_exception(msg, macro, tag) %}
{#
    "Raises formatted DBT compiler error
    "Arg:
    "   - msg: str   -- Error message
    "   - macro: str -- Failed macro name
    "   - tag: str   -- Extra marker which could help to more accurately spot the error place
    "Raises:
    "    exceptions.raise_compiler_error
#}
    {% if macro and tag %}
        {{ exceptions.raise_compiler_error(macro ~ " [" ~ tag ~ "] :: " ~ msg) }}
    {% elif macro %}
        {{ exceptions.raise_compiler_error(macro ~ " :: " ~ msg) }}
    {% else %}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}
{% endmacro %}
