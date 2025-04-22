{% macro mtf_resolve_transform_model_src(tf_config) %}
{#
    "Resolves transformation dependencies from provided tf_config
    "Arg:
    "   - tf_config: dict
    "Returns:
    "   Relation
    "Warn:
    "   In case found dependency is DBT Ref and the Ref is not directly mentioned in the target model sql
    "   DBT compile will require the current model to provide information about the dependency via the following instruction:
    "       --depends_on: {{ ref('DBT_Ref_Id') }}
#}
    {% set result =  {} %}
    {% if tf_config.source %}
        {{ result.update({"source": mtf_find_source(tf_config.source)}) }}
        {% if not result.source %}
            {{ result.update({"source": mtf_find_model(tf_config.source)}) }}
        {% endif %}
    {% else %}
        {{ exceptions.raise_compiler_error("`source` must be defined in " ~ tf_config) }}
    {% endif %}
    {% if result.source %}
        {{ return(result.source) }}
    {% else %}
        {{ exceptions.raise_compiler_error("[" ~ tf_config.source ~ "] not found") }}
    {% endif %}
{% endmacro %}
