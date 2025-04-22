{% macro list_to_dict(items, id_keys, delimeter='.') %}

    {#
        Returns a dict of the provided list of dicts in form {id: tuple(list[i]), ...},
        where id is assembled out of specified item keys values
        Note: if there is an id collision the latest list entry is returned
     #}

    {% set result={} %}

    {% if execute %}
        {% for item in items %}
            {% set id_values = [] %}
            {% for id_key in id_keys %}
                {% do id_values.append(item[id_key]) %}
            {% endfor %}
            {% set item_id = delimeter.join(id_values) %}
            {% do result.update({item_id : item}) %}
        {% endfor %}
    {% endif %}

    {{ return(result) }}

{% endmacro %}
