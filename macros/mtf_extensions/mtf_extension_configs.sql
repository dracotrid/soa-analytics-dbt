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
            "NUMERIC.AMOUNT": "NUMERIC(10,2)"
        }
    %}
    {{ return(custom_data_types) }}
{% endmacro %}
