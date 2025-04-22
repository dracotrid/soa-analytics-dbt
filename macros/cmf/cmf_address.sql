{#
    "Core Model Foundation Address helper functions
#}

{% macro cmf__country_alpha_3(country_name) %}
{#
    "Converts provided country name into ALPHA-3 code (ISO 3166-1) otherwise returns NULL
    "COUNTRY CODES ALPHA-3: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
    "
    "TODO: Introduce a dedicated DS/FM for system/common dictionaries
#}
CASE {{ country_name }}
  WHEN "Germany" THEN "DEU"
  WHEN "Singapore" THEN "SGP"
  WHEN "United Kingdom" THEN "GBR"
  WHEN "United Kingdom of Great Britain and Northern Ireland" THEN "GBR"
  WHEN "United States" THEN "USA"
  WHEN "United States of America" THEN "USA"
  WHEN "India" THEN "IND"
  ELSE NULL
END
{% endmacro %}


{% macro cmf__usa_state_alpha_2(state_name) %}
{#
    "Converts provided USA state name into ALPHA-2 code (ISO 3166-2) otherwise returns NULL
    "CODES ALPHA-2: https://en.wikipedia.org/wiki/ISO_3166-2:US
    "
    "TODO: Introduce a dedicated DS/FM for system/common dictionaries
#}
CASE {{ state_name }}
    WHEN 'Alabama' THEN 'AL'
    WHEN 'Alaska' THEN 'AK'
    WHEN 'Arizona' THEN 'AZ'
    WHEN 'Arkansas' THEN 'AR'
    WHEN 'California' THEN 'CA'
    WHEN 'Colorado' THEN 'CO'
    WHEN 'Connecticut' THEN 'CT'
    WHEN 'Delaware' THEN 'DE'
    WHEN 'Florida' THEN 'FL'
    WHEN 'Georgia' THEN 'GA'
    WHEN 'Hawaii' THEN 'HI'
    WHEN 'Idaho' THEN 'ID'
    WHEN 'Illinois' THEN 'IL'
    WHEN 'Indiana' THEN 'IN'
    WHEN 'Iowa' THEN 'IA'
    WHEN 'Kansas' THEN 'KS'
    WHEN 'Kentucky' THEN 'KY'
    WHEN 'Louisiana' THEN 'LA'
    WHEN 'Maine' THEN 'ME'
    WHEN 'Maryland' THEN 'MD'
    WHEN 'Massachusetts' THEN 'MA'
    WHEN 'Michigan' THEN 'MI'
    WHEN 'Minnesota' THEN 'MN'
    WHEN 'Mississippi' THEN 'MS'
    WHEN 'Missouri' THEN 'MO'
    WHEN 'Montana' THEN 'MT'
    WHEN 'Nebraska' THEN 'NE'
    WHEN 'Nevada' THEN 'NV'
    WHEN 'New Hampshire' THEN 'NH'
    WHEN 'New Jersey' THEN 'NJ'
    WHEN 'New Mexico' THEN 'NM'
    WHEN 'New York' THEN 'NY'
    WHEN 'North Carolina' THEN 'NC'
    WHEN 'North Dakota' THEN 'ND'
    WHEN 'Ohio' THEN 'OH'
    WHEN 'Oklahoma' THEN 'OK'
    WHEN 'Oregon' THEN 'OR'
    WHEN 'Pennsylvania' THEN 'PA'
    WHEN 'Rhode Island' THEN 'RI'
    WHEN 'South Carolina' THEN 'SC'
    WHEN 'South Dakota' THEN 'SD'
    WHEN 'Tennessee' THEN 'TN'
    WHEN 'Texas' THEN 'TX'
    WHEN 'Utah' THEN 'UT'
    WHEN 'Vermont' THEN 'VT'
    WHEN 'Virginia' THEN 'VA'
    WHEN 'Washington' THEN 'WA'
    WHEN 'West Virginia' THEN 'WV'
    WHEN 'Wisconsin' THEN 'WI'
    WHEN 'Wyoming' THEN 'WY'
  ELSE NULL
END
{% endmacro %}


{% macro cmf__usa_location_to_state_alpha_2(location) %}
{#
    "Converts provided USA location into ALPHA-2 code (ISO 3166-2) otherwise returns NULL
    "CODES ALPHA-2: https://en.wikipedia.org/wiki/ISO_3166-2:US
    "
    "TODO: Introduce a dedicated DS/FM for system/common dictionaries
#}
CASE {{ location }}
    WHEN 'Bellevue, Washington' THEN 'WA'
    WHEN 'Cambridge' THEN 'MA'
    WHEN 'Florida' THEN 'FL'
    WHEN 'Jackson Hole, Wyoming' THEN 'WY'
    WHEN 'New York' THEN 'NY'
    WHEN 'Palo Alto' THEN 'CA'
    WHEN 'San Francisco' THEN 'CA'
    WHEN 'Utah' THEN 'UT'
    WHEN 'Washington D.C.' THEN 'WA'
  ELSE NULL
END
{% endmacro %}
