#!/bin/bash

export GCP_DEFAULT_REGION_ZONE=us-central1-a

# DBT-related variables
export SOA_DBT_GOOGLE_BIGQUERY_KEYFILE=/path/tj/soa-application_default_credentials.json

export SOA_DBT_GOOGLE_PROJECT_DEV=solomia-analytics-main
export SOA_DBT_GOOGLE_BIGQUERY_DATASET_DEV=soa_dev_[sdv]

export SOA_DBT_GOOGLE_BIGQUERY_KEYFILE_PROD=/path/tj/soa-application_default_credentials.json
export SOA_DBT_GOOGLE_PROJECT_PROD=solomia-analytics-main
export SOA_DBT_GOOGLE_BIGQUERY_DATASET_PROD=soa_prod
