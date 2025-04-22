{#
    "Contains Core Model Foundation value normalisation helpers:
    "   - cmf__normalise__phone_number
#}


{% macro cmf__normalise__phone_number(phone_number_field, country_field) %}
{#
    "Normalises phone numbers to digits-only representation and adds country code if necessary
    "
    " Supported formats:
    "  - USA:   [+1](XXX) XXX-XXXX -> +1XXXXXXXXXX
    "  - UK:    +44 (XXXX) XXX-XXX -> +44XXXXXXXXXX
    "  - Other: +X (XX) XXX-XXX     -> XXXXXXXXX
    "
    "Output: str, digits only, prefixed with '+' if a number is completely normalised
#}
CASE
    {# "If work_phone starts with '+'" #}
    WHEN REGEXP_CONTAINS({{ phone_number_field }}, r'^\+') THEN
        CONCAT('+', REGEXP_REPLACE({{ phone_number_field }}, r'\D', ''))

    {# "If phone_number_field matches (XXX) XXX XXXX and X is a digit then it is USA phone" #}
    WHEN REGEXP_CONTAINS({{ phone_number_field }}, r'^\(?\d{3}\)?.{1}\d{3}.{1}\d{4}$') THEN
      CONCAT('+1', REGEXP_REPLACE({{ phone_number_field }}, r'\D', ''))

    {# "If phone_number_field matches (XXXX) XXX XXX and X is a digit then it is UK phone" #}
    WHEN REGEXP_CONTAINS({{ phone_number_field }}, r'^\(?\d{4}\)?.{1}\d{3}.{1}\d{3}$') THEN
      CONCAT('+44', REGEXP_REPLACE({{ phone_number_field }}, r'\D', ''))

    {# "If phone_number_field contains exactly 10 digits and starts with USA area codes then it is USA phone" #}
    WHEN LENGTH(REGEXP_REPLACE({{ phone_number_field }}, r'\D', '')) = 10
      AND (
        (REGEXP_CONTAINS(REGEXP_REPLACE({{ phone_number_field }}, r'\D', ''), r'^(212|415|425|617|646|650|917|925)'))
        OR ({{ country_field }} IN ('USA', 'United States', 'United States of America'))
      ) THEN
        CONCAT('+1', REGEXP_REPLACE({{ phone_number_field }}, r'\D', ''))

    {# "Otherwise, just keep only the digit symbols" #}
    ELSE
      REGEXP_REPLACE({{ phone_number_field }}, r'\D', '')
END
{% endmacro %}
