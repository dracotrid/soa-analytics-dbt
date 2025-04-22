{%- macro tf_transform_model(tf_source, tf_config) -%}
{#
    "Transforms provided source with provided transformation configuration
    "Warn: Works only in DBT EXECUTE mode
    "
    "  Args:
    "    tf_source: Union[Relation, CTE_name:str, List[Union[Relation, CTE_name:str]]]
    "    tf_config: Optional[dict]  -- if not provided expected to be assembled based on current model config
    " --- Embedded TF config ---
    " < Check out ReadMe for more details >
    " Model level tf_config
    "    models:
    "        - name: <model name>
    "            ...
    "            config:
    "                meta:
    "                    tf_config: dict
    " Model Column level tf_config
    "    column:
    "        ...
    "        meta:
    "            tf_config: dict
    " --- Standalone TF config ---
    "    source: str -- Source Name
    "    target: str -- Target model name
    "    mode: Optional[str], default is 'flat'
    "   ...: Any -- other parameters depend on transformation 'mode'
#}
    {%- if execute -%}
        {%- if tf_source and tf_config -%}
            {%- set transform_fn = mtf_resolve_transform_model_fn(tf_config) -%}
            {{ transform_fn(tf_source, tf_config) }}
        {%- elif tf_source -%}
            {%- set dbt_this_model = builtins.model -%} {# "Loads current DBT model" #}
            {%- set tf_config = mtf_resolve_transform_model_conf(dbt_this_model) -%}
            {{ tf_transform_model(tf_source, tf_config) }}
        {%- else -%}
            {#- "TODO: review the need of model creation based solely on configuration  " -#}
            {%- set tf_config = mtf_resolve_transform_model_conf(model) -%}
            {%- set tf_source = mtf_resolve_transform_model_src(tf_config) -%}
            {#- "{{ tf_transform_model(tf_source, tf_config) }}" -#}
            {{- exceptions.raise_compiler_error("`tf_source` is required") -}}
        {%- endif %}
    {%- endif %}
{%- endmacro -%}
