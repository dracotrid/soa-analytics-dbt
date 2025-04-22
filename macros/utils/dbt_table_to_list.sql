{% macro dbt_table_to_list(dbt_table) %}

    {# This macro returns a list of the provided Table rows in form [{col_name1: value, col_name2: value,}, ] #}

    {% set result=[] %}

    {% for row in dbt_table.rows %}
        {% set row_dict={} %}
        {% for column_name in dbt_table.column_names %}
            {% do row_dict.update({column_name: row[column_name]}) %}
        {% endfor %}
        {% do result.append(row_dict) %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
