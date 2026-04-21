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

{% macro smmsales__resolve__ad_name(raw_name) %}
{#
    Extract original ad name
    TODO: Load from external configuration
#}
CASE
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія, довгий_ПОПМУЗИКА", "Подологія, ПОП")  }}
  ELSE {{ raw_name }}
END
{% endmacro %}


{% macro smmsales__resolve__ad_creative(raw_name) %}
{#
    Extract ad creative code
    TODO: Load from external configuration
#}
CASE
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія, старий", "3881")  }}
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія, Сева", "4348")  }}
  {#- Подологія_Квітень -#}
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія_Квітень_БезТ", "1529")  }}
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія_Квітень_БілаТ", "9603")  }}
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія_Квітень_ФіолТ", "9604")  }}
  {#- Подологія, ПОП -#}
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія, довгий_ПОПМУЗИКА", "7679")  }}
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія, ПОП [,]", "7679")  }}
  {{ smmsales_lh_when_starts_with(raw_name, "Подологія, ПОП тюльпан", "7679")  }}


  ELSE {{ raw_name }}
END
{% endmacro %}


{% macro smmsales__extract__canonical_ad_name(full_name) %}
{#
    Extract original ad name
    TODO: Load from external configuration
#}
{%- set ad_name = smmsales__resolve__ad_name(full_name) -%}
CASE
  {{ smmsales_lh_when_starts_with(ad_name, "Подологія, старий")  }}
  {{ smmsales_lh_when_starts_with(ad_name, "Подологія, ПОП")  }}
  {{ smmsales_lh_when_starts_with(ad_name, "Пальці")  }}
  {{ smmsales_lh_when_starts_with(ad_name, "Подологія_Квітень")  }}
  {{ smmsales_lh_when_starts_with(ad_name, "Подологія, старий")  }}
  ELSE {{ ad_name }}
END
{% endmacro %}


{% macro smmsales__extract__ad_audience(full_ad_name, date) %}
{#
    Extract original ad name
    TODO: Load from external configuration
#}
CASE
    WHEN {{ full_ad_name }}  = "Подологія, Коновалова" THEN CASE
        WHEN {{ date }} <= "2025-05-30" THEN "MIXED"
        ELSE "_TBD_[Подологія, Коновалова]"
    END
    {{ smmsales_lh_when_equals(full_ad_name, "Подологія, старий [.]", "ПС [.]") }}
    {{ smmsales_lh_when_equals(full_ad_name, "Подологія, старий [..]", "ПС [..]") }}
    {{ smmsales_lh_when_equals(full_ad_name, "Подологія_Квітень_БезТ", "ПК БезТ") }}
    {{ smmsales_lh_when_equals(full_ad_name, "Подологія_Квітень_БілаТ", "ПК БілаТ") }}
    {{ smmsales_lh_when_equals(full_ad_name, "Подологія_Квітень_ФіолТ", "ПК ФіолТ") }}

    {{ smmsales_lh_when_equals(full_ad_name, "Подологія, довгий_ПОПМУЗИКА", "ПОП") }}
    {{ smmsales_lh_when_equals(full_ad_name, "Подологія, ПОП [,]", "ПОП [,]") }}
    {{ smmsales_lh_when_equals(full_ad_name, "Подологія, ПОП тюльпан", "ПОП [Т]") }}
  ELSE "UNDEFINED"
END
{% endmacro %}

-- LOCAL HELPERS --

{% macro smmsales_lh_when_starts_with(field_name, prefix, output) %}
{%- set result = output if output else prefix -%}
WHEN STARTS_WITH(LOWER({{ field_name }}), LOWER(TRIM('{{ prefix }}'))) THEN '{{ result }}'
{% endmacro %}


{% macro smmsales_lh_when_equals(field_name, input_value, output_value) %}
WHEN LOWER({{ field_name }}) = LOWER(TRIM('{{ input_value }}')) THEN '{{ output_value }}'
{% endmacro %}
