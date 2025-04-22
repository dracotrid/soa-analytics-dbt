#!/bin/bash

export GCP_DEFAULT_REGION_ZONE=us-central1-a

# DBT-related variables
export SOA_DBT_GOOGLE_BIGQUERY_KEYFILE_DEV=
export SOA_DBT_GOOGLE_PROJECT_DEV=solomia-analytics-main
export SOA_DBT_GOOGLE_BIGQUERY_DATASET_DEV=soa_dev_sdv

export SOA_DBT_GOOGLE_BIGQUERY_KEYFILE_PROD=""
export SOA_DBT_GOOGLE_PROJECT_PROD=solomia-analytics-main
export SOA_DBT_GOOGLE_BIGQUERY_DATASET_PROD="soa_prod"
