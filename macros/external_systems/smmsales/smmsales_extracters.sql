{#
    Contains value extract helpers from SMM Sales raw data:
        - smmsales__extract__original_ad_name
#}

{% macro smmsales__extract__source_type(communication_source) %}
{#
    "Extract communication source type
#}
CASE
  WHEN LOWER({{ communication_source }}) = "звернення клієнта" THEN "CLIENT_REQUEST"
  WHEN LOWER({{ communication_source }}) = "відповідь на коментар" THEN "CLIENT_COMMENT"
  WHEN LOWER({{ communication_source }}) = "запит по рекламі, але не зрозуміло яка" THEN "UNKNOWN_AD"
  WHEN LOWER({{ communication_source }}) = "нагадування" THEN "REMINDER"
  WHEN {{ communication_source }} IS NULL THEN "UNKNOWN"
  ELSE "AD"
END
{% endmacro %}

{% macro smmsales__extract__ad_name(communication_source) %}
{#
    "Extracts ad name
#}
CASE ({{ smmsales__extract__source_type(communication_source) }})
  WHEN "AD" THEN {{ communication_source }}
  ELSE NULL
END
{% endmacro %}

{% macro smmsales__extract__original_ad_name(full_ad_name) %}
{#
    "Extract original ad name
#}
CASE
  {{ smmsales_lh_when_starts_with(full_ad_name, "Подологія, старий")  }}
  {{ smmsales_lh_when_starts_with(full_ad_name, "Подологія, ПОП")  }}
  {{ smmsales_lh_when_starts_with(full_ad_name, "Пальці")  }}
  {{ smmsales_lh_when_starts_with(full_ad_name, "Подологія_Квітень")  }}
  {{ smmsales_lh_when_starts_with(full_ad_name, "Подологія, старий")  }}
  ELSE {{ full_ad_name }}
END
{% endmacro %}

-- LOCAL HELPERS --

{% macro smmsales_lh_when_starts_with(field_name, prefix) %}
WHEN STARTS_WITH(LOWER({{ field_name }}), LOWER(TRIM('{{ prefix }}'))) THEN '{{ prefix }}'
{% endmacro %}
