{#
    Contains MTF config extensions:
        - mtf__extension__config__base_type
        - mtf__extension__config__custom_data_types
#}


{% macro mtf__extension__config__base_type_extractor(custom_type) %}
{#
    Returns database base type of a custom type provided
#}
    {{ return(custom_type.split('.')[0]) }}
{% endmacro %}


{% macro mtf__extension__config__custom_data_types() %}
{#
    Returns mapping of custom types to database supported data types
#}
    {% set custom_data_types = {
            "NUMERIC.COST": "NUMERIC(12,2)",
            "NUMERIC.PRICE": "NUMERIC(15,5)",
            "NUMERIC.AMOUNT": "NUMERIC(10,2)",
            "STRING.CODE": "STRING"
        }
    %}
    {{ return(custom_data_types) }}
{% endmacro %}

{% macro mtf__extension__config__target_type_value_resolver(target_type, tf_source_expression, debug=false) %}
    {%- set _macro_ = "mtf__extension__config__target_type_value_resolver" -%}
    {%- set _ = log(
            _macro_ ~ " :: input: " ~ {"tf_source_expression": tf_source_expression, "target_type": target_type},
            info=true
        ) if debug else ""
    -%}

    {% if target_type == "STRING.CODE" %}
        {% set _tf_source_expression = "LPAD(TRIM(CAST(CAST(CAST("+tf_source_expression+" AS STRING) AS INT) AS STRING)), 10, '0')" %}
    {% else %}
        {% set _tf_source_expression = tf_source_expression %}
    {% endif %}

    {{ return(_tf_source_expression) }}
{% endmacro %}
