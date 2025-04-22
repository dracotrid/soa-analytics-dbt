# Model Transformation Framework
**Disclaimer**: _That's a very first version the framework capabilities so it's going to be enhanced in next revisions_

MTF (Model Transformation Framework) changes the way you take DBT Relation [properties & configs](https://docs.getdbt.com/reference/configs-and-properties)
in your project from _nice-to-have_ to **must-have** which brings your project maturity to another level making sure the schema of a DBT model you define 
in your DBT project always matches to a Storage Asset created by DBT engine in your target Data Storage

## Example 1

Let's consider having the following DBT Source 
```
name: my_raw_items
schema:
    - id: int
    _ type_code: string
    - name: string
    - price: string
    - date: string (format is, i.e. YYYY-MM-DD)
    - _system_: string
```
Following best practices of Source data preparation and your project requirements you identify you need to:
1. Change `id` field type from `int` to `string`
2. Make sure `type` and `name` are trimmed
3. Rename `name` to `item_code`
4. Change `price` field type from `string` to `numeric(12,6)`
5. Change `date` field type from `string` to `date`
6. Exclude `_system_` field
7. Make sure your model contains only `id`, `type_code`, `item_code`, `price` and `date` fields
8. Test id is unique
9. Document your model

### DBT native approach

With DBT imperative-declaration-first approach you implement the following DBT-script which covers steps 1-7
```SQL
-- moodel name: my_parsed_items
SELECT
    CAST(id AS STRING) AS id,
    TRIM(type_code) AS type_code,
    TRIM(name) AS item_code,
    CAST(TRIM(price) AS NUMERIC) AS price,
    CAST(TRIM(date) AS DATE) AS date
FROM
    {{ source('source_ns', 'my_raw_items') }}
```
For the last (9) step you decide to generate dbt-doc with i.e. dbt-osmosis
```YAML
# moodel name: my_parsed_items
version: 2

models:
  - name: my_parsed_items
    columns:
      - name: id
        data_type: STRING
      - name: type_code
        data_type: STRING
      - name: item_code
        data_type: STRING
      - name: price
        data_type: NUMERIC
      - name: date
        data_type: DATE
```
For the step (7)  you adjust the yaml
```YAML
# moodel name: my_parsed_items
version: 2

models:
  - name: my_parsed_items
    columns:
      - name: id
        data_type: STRING
        tests:
          - unique
      - name: type_code
        data_type: STRING
      - name: item_code
        data_type: STRING
      - name: price
        data_type: NUMERIC
      - name: date
        data_type: DATE
```
You notice `price` is not exactly of the type you need -- `NUMERIC` instead of `NUMERIC(12,6)` so you adjust the yaml
```YAML
# moodel name: my_parsed_items
version: 2

models:
  - name: my_parsed_items
    columns:
      - name: id
        data_type: STRING
      - name: type_code
        data_type: STRING
      - name: item_code
        data_type: STRING
      - name: price
        data_type: NUMERIC(12,6)
      - name: date
        data_type: DATE
```
You notice `price` in your database is still of `NUMERIC` type so you adjust the yaml
```YAML
# moodel name: my_parsed_items
version: 2

models:
  - name: my_parsed_items
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: STRING
      - name: type_code
        data_type: STRING
      - name: item_code
        data_type: STRING
      - name: price
        data_type: NUMERIC(12,6)
      - name: date
        data_type: DATE
```
**Done!**


### MTF approach
You define your target model schema which covers steps (1-9)
```YAML
# moodel name: my_parsed_items
version: 2

models:
  - name: my_parsed_items
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: STRING
        tests:
          - unique
      - name: type_code
        data_type: STRING
      - name: item_code
        data_type: STRING
        meta:
          tf_config:
            field: name
      - name: price
        data_type: NUMERIC(12,6)
      - name: date
        data_type: DATE
```
You define boilerplate model sql
```SQL
-- moodel name: my_parsed_items
{{ tf_transform_model(source('source_ns', 'my_raw_items')) }}
```
**Done!**
