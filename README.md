# DBT Usage Guide

## Guides
- [Model Transformation Framework](./macros/mtf/README.md)

## Local Environment Setup

### Python
This project requires Python 3.11 (specifically 3.11.5) to be run on.
There are various ways to install Python on your environment, we use [`pyenv`](https://github.com/pyenv/pyenv) across
all our projects

**Prerequisites**:
1. It is assumed you've already cloned the repo and navigated to the repo folder in your favorite Terminal
2. (Windows only) You've got [`Microsoft Visual C++ 14.0 or greater`](https://wiki.python.org/moin/WindowsCompilers#Microsoft_Visual_C.2B-.2B-_14.0_standalone:_Visual_C.2B-.2B-_Build_Tools_2015_.28x86.2C_x64.2C_ARM.29) installed

**Setup steps**:
1. Install PyEnv
   - mac/linux: [`pyenv`](https://github.com/pyenv/pyenv)
   - windows: [`pyenv-win`](https://github.com/pyenv-win/pyenv-win)
     - reference: [pyenv-win & virtualenv – Windows](https://rkadezone.wordpress.com/2020/09/14/pyenv-win-virtualenv-windows/)
2. Install Python 3.11.5: `pyenv install 3.11.5`
3. Set default python for the project: `pyenv local 3.11.5`
4. Upgrade pip: `python -m pip install --upgrade pip`
5. Upgrade setuptools: `pip install --upgrade pip setuptools`
6. Install Python Virtual Environment plugin
   - mac/linux: [`pyenv-virtualenv`](https://github.com/pyenv/pyenv-virtualenv)
   - windows/mac/linux: [`virtualenv`](https://virtualenv.pypa.io/en/latest/installation.html)
     - `set-executionpolicy remotesigned` (Windows only)
     - `pip install virtualenv`
7. Create Python Virtual Environment for the project. _Note_: feel free to pick up any name instead of `soa-3.11-dbt` and
`env` (checkout the latter is added to `.gitignore`)
   - pyenv-virtualenv: `pyenv virtualenv 3.11.5 soa-3.11-dbt`
   - virtualenv: `python -m venv env`
8. Activate Python Virtual Environment
    - pyenv-virtualenv:
      - zshrc: `pyenv local soa-3.11-dbt && source ~/.zshrc`
      - bash: `pyenv local soa-3.11-dbt && source ~/.bashrc`
    - virtualenv:
      - mac/linux: `source env/bin/activate`
      - windows: `.\env\Scripts\activate`
9. Install the project dependencies: `pip install -r requirements.txt`
10. Install pre-commit hooks: `pre-commit install`
11. Install DBT dependencies: `dbt deps`
12. Congratulation! You've finished with Python configuration on your local env. There are just a few steps left to finish up complete development setup:
  - Get your GCP credentials with `Using soa-cli.sh` section below


### Using soa-cli.sh
Bash script `soa-cli.sh` automates the steps described in the later sections.
You can login with `./bin/soa-cli.sh login`

Before running login, please execute the following thing:

```
>>> chmod +x ./bin/soa-cli.sh
```
For more info execute:

```
>>> ./bin/soa-cli.sh help
```

### Connecting to BigQuery
To connect to BigQuery using your favorite DB Client you need to login to GCP with either method described above and set the JDBC connection parameters. E.g. (for DataGrip):

- _Authentication_: `Application Default Credentials`
- _Project ID_: `solomia-analytics-main`
- _Service account key file_: `$HOME/.config/gcloud/application_default_credentials.json` (replace **$HOME** with the absolute path to your home folder)
- _Default dataset_: `<empty>`
- _URL_: `jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443?RequestGoogleDriveScope=1`

### dbt-power-user setup (VSCode extension)

This extension makes vscode seamlessly work with dbt.
See examples of what it can do [here](https://github.com/AltimateAI/vscode-dbt-power-user/blob/master/README.md).

#### Setup steps

1. After opening a project you will be prompted to install recommended extensions. Install them.
2. Go in `Code > Preferences > Settings… > User`
Search for `Python info visibility` Set this setting as `Always`.
3. Run `cp .vscode/settings.template.json .vscode/settings.json`.
4. Set default Python interpreter for the workspace. To do that add `python.defaultInterpreterPath` to `.vscode/settings.json`.
  _Example_:

   ```json
   ...
   "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python",
   ...
   ```

5. Login to GCP with `./bin/soa-cli.sh login`. Add env vars from `login` output (_without export keyword_) to `.env` file in the root of the project.
6. Re-run `dbt deps`.
7. Restart VSCode.
8. Use `cmd+shift+p` to open the command palette and run `View: Show dbt Power User` to open the extension's panel. Click on `Project Actions -> Project Health Check -> Start Scan` to check connectivity.
  ![Alt text](<docs/img/dbt_power_user_health_check.png>)
9. Run `which sqlfluff` with your `venv` activated and fill in the output into `sqlfluff.executablePath` setting in `.vscode/settings.json`.
  _Example_:

   ```json
   ...
   "sqlfluff.executablePath": "${workspaceFolder}/venv/bin/sqlfluff",
   ...
   ```

### First run

> Build:
```bash
dbt build --profiles-dir ./ --target unit-test --vars 'unit_testing: true'
```
Build will compile your models in a SQL code.

> Run:
```bash
dbt run --profiles-dir ./ --target unit-test --vars 'unit_testing: true'
```
Run will apply compiled SQL code and write data to the storage.

###### !! Make sure you properly set environment variables with Data Warehouse credentials locally, before running.

Example of setting env variables

```bash
export SOA_DBT_GOOGLE_BIGQUERY_KEYFILE="path/to/key/soa-service-key.json"
export SOA_DBT_GOOGLE_PROJECT="soa"

export SOA_DBT_GOOGLE_BIGQUERY_DATASET_DEV="soa_prod_user_name"
export SOA_DBT_GOOGLE_BIGQUERY_DATASET_PROD="soa_prod"
export SOA_DBT_GOOGLE_BIGQUERY_DATASET_TEST="soa_unit_test_db"
```


### Description:
`--profiles-dir` - tag where to look for `profiles.yml` file. This file has configuration variables for DB connection and setup.

`--target`- tag what configuration from profiles.yml to use. Currently available: `dev`, `prod`, `unit-test`.

`--vars` - tag for environment variables.

`--exclude` - which tags (models, seeds, etc to exclude in current run.) Any model can be tagged.

### Developing Models

To create a new model (table / view) you need:

1) Create new file in the models folder. You can also create subfolder to organize your models better (like `models/transactions` or `models/reports`). File name will be used as a resulting table/view name.

2) Write SQL query for your model.

3) Add fields and properties for them in the `models/schema.yml`

4) Lint models with `sqlfluff lint models`

5) Try to `dbt build` and fix the errors if any appears.


### Adding Sources

To add new source (table in warehouse) add it to the `models/sources.yml` file:
```
sources:
    - name: soa_prod # this is the source_name (will be used also as identifier)
      database: "{{ var('primary_bigquery_database_name') }}"  # Poject name in BigQuery case
      schema: soa_prod # Database schema to use

      tables:
        - name: transactions # table name will be used to refer source
          identifier: TransactionLedger # real table name in a warehouse
```

Referering source in a model:
```sql
select * from {{ source('soa_prod', 'aumni_transactions') }}
```


### Adding Static Data

In cases when you need to insert some static mapping tables in DBT workflow, you can use Seeds. Just put .CSV file with your data in seeds folder and run:
```bash
dbt seed --full-refresh --profiles-dir ./ --target dev --vars 'unit_testing: false' --exclude tag:unit_testing
```

### Referencing other models

To reference any other model in DBT just put it in brackets {{ }}
For example to reference model `exited_investments_by_fund` use ``{{ ref('exited_investments_by_fund') }}``

If you need to reference table outside from DBT, store it in the variables at `dbt_project.yml`
Variables can be acessed with `var` keyword: ``{{ var('max_date') }}``

### Adding documentation for models

DBT can automatically create a documentation using provided description of tables and columns.
To generate documentation run:
```bash
dbt docs generate
```
and to start server with a documentation locally:
```bash
dbt docs serve
```

### Documentation Creation Rules

1) Create folder `meta` under your directory. For example, if you are adding properties for `models/transactions`, you need to create folder `models/transactions/meta`

2) For each entity (model, source, seed) add a corresponding YAML file.
3) Required fields for each model / column / source / etc:
  - name
  - description

4) Example of structure:
```YAML
version: 2

models:
  - name: cashflow_report
    description: "Salesforce based Cashflow Data for each fund."
    columns:
      - name: id
        description: "Unique Fund ID (Salesforce)"
      - name: fund_name
        description: "Unique Fund Name (Salesforce)"
      - name: total_distribution_contribution
        description: "Total Distribution/Contribution"
      - name: lp_distribution_contribution
        description: "LP Distribution/Contribution"
      - name: cashflow_date
        description: "Cash Flow Date"
```
> !!! Important: `version: 2` should be always on top of each your YAML file for DBT to be able to parse it.

5) If you need to write detailed documentation, you can create a Markdown file in the `docs` folder and then reference it in your properites file like this:
```YAML
version: 2

models:
  - name: net_irr_by_fund
    description: '{{ doc("xirr_description") }}'
```


### Tests

Official dbt documentation https://docs.getdbt.com/docs/build/tests

Our workflow:
  1) Tests are added to the corresponding views and tables using meta YML files.
  For example to add tests for column Company_id in model `matched_transactions` we need to put rule in `matched_transactions.yml` file:

  ```YAML
  ...
  name: Company_id
    description: "Company Id"
    tests:
      - not_null
  ...
  ```
  2) Generic tests can be added simply (https://docs.getdbt.com/docs/build/tests#generic-tests)
  like in example above - to any specific columns you want to be tested.


  3) A lot of tests and useful functions can be added using DBT Utils https://github.com/dbt-labs/dbt-utils

  4) **Complex tests**

  Let's see an example of `updated_deals` test.

  Test case:
  - Transaction from `aumni_transaction_based_deals` was matched sucessfully (manually or automatically). But after some period of time, we retrieve same trancaction (with same ID) but some of important data was changed (for example round_size is different now). Then we need to put this transaction to the unmatched deals.

To check this situation we need to create test tables with simulation of this context.
To create simulated test tables we need to add `.csv` files with example data in the `seeds/{test_case_name}` folder. In our case it is `seeds/updated_deals/`.

Each test table should be named same as original one but with `_test` suffix.

Use `select_table` macros in a model you want to test:

```SQL
-- unmatched_transactions.sql

{% set matched_transactions = select_table('matched_transactions') %}
{% set aumni_deals = select_table('aumni_transaction_based_deals') %}
{% set manual_deals_table = select_table('manually_merged_sf_aumni_transactions',
   use_source=true, schema='soa_prod') %}

```

This code will use original tables in a standart DBT run mode, and test tables in a test mode.

Test flow:
- Build a model unmatched_transactions using simulated data
- Changed transactions should appear in an unmatched model
- So we compare what model outputs with the expected result in `seeds/updated_deals/unmatched_expected.csv`

In `unmatched_transactions.yml` it looks like:

```YAML
tests:
  - dbt_utils.equality:
      compare_model: ref('unmatched_expected')
      compare_columns:
        - aumni_id
      tags: ['unit_testing']
```

description:

1. `dbt_utils.equality:` - set up a test on table equality
2. `compare_model: ref('unmatched_expected')` - set table to compare with (unmatched_expected in this case)
3.  `compare_columns: - aumni_id` - list columns to compare on.
4. `tags: ['unit_testing']` - add unit testing tag, so this test will only be run in a testing mode.


Run tests:
```
dbt build --profiles-dir ./ --target unit-test --vars 'unit_testing: true'

dbt run --profiles-dir ./ --target unit-test --vars 'unit_testing: true'
```



# DBT-Osmosis

## Why we need this:
- DBT Osmosis automatically generates the YML documentation for your models using target model schema.
- You don’t need to write alll of the columns manually in the YML file, Osmosis will add them automatically, it Exactly same order like in the resulting table.
- Documentation is automatically up to date.
- Osmosis will trigger errors if there are duplicated models, missing files or wrongly named ones.
- Osmosis will do documentation inheritance: if your models rely on a few sources, you can just add descriptions to those sources, and osmosis will automatically fill related models with appropriate descriptions.
- Osmosis can also infer and add TYPE to the documentation.

## Proposed workflow:
1) Install Osmosis in your system. See Install block.

2) Do your model changes, add new models, etc.

3) Apply models to your testing database target (Run `dbt build`, `dbt run`)
 -- Note: Osmosis WILL NOT DELETE any model, you should do it manually if required.

4) Run Osmosis. See Usage block.

5) Resolve errors if any appears. See Known errors and limitations.

6) Commit & Push

7) Done!


## Install

Just run `python -m pip install -r requirements.txt`

## Usage

Main commands to use (Refactor)

```bash
python -m pipx run --pip-args="dbt-bigquery" dbt-osmosis yaml [--project-dir] [--profiles-dir] [--target]
```

Example usage:
```bash
python -m pipx run --pip-args="dbt-bigquery" dbt-osmosis yaml refactor models/staging/mailchimp --target prod
```

!!! Important: You should use the target, where your models are already build! If you did a local changes only, you should provide the same target as when you did DBT build.


### ‼ Known errors and limitations:
- KeyError: 'sources'

There is YML file with such model already, but probably named wrongly or with a typo. Search for last listed file in the output, and see if there are any other files, that use this model name or file name is wrong.

- KeyError ‘model’

There is model with the same name somewhere in the project already. Maybe duplicated file, or smthg like that.

- Osmosis can’t reach source with a different identifier than a source name. (Work in progess withing PR in the project.)

## Configuration (Danger zone)
Configuration is maintained using dbt_project.yml

Configuration sets the path where to create documentation. You DON’T HAVE TO change configuration without a major reason, and without team approval.

Current configuration is set as is, to build YML files in the meta folders nearby models.


```YAML
models:
  soa:
    +dbt-osmosis: "meta/{model}.yml"
```

!!! If you change configuration - running Osmosis refactor will RE-GENERATE ALL META FILES in a new path, according to your new configuration. Old docs will not be deleted!!!
