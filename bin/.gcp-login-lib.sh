#!/bin/bash

function login {
  DEFAULT_PROJECT=solomia-analytics-main

  gcloud auth application-default login --scopes=openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/drive
  gcloud config set project $DEFAULT_PROJECT
  gcloud auth login --update-adc --enable-gdrive-access

  normalized_user_name=$(gcloud config get account | sed -E "s/(.+)@(.*)/\1/" | tr '.' '_' | tr '-' '_')

  DBT_GOOGLE_BIGQUERY_KEYFILE="$HOME/.config/gcloud/application_default_credentials.json"
  DBT_GOOGLE_PROJECT=$DEFAULT_PROJECT
  DBT_GOOGLE_BIGQUERY_DATASET_DEV="soa_dev_$normalized_user_name"
  DBT_GOOGLE_BIGQUERY_DATASET_PROD="soa_prod"

  printf "\nCopy and execute in your terminal session:
    export SOA_DBT_GOOGLE_BIGQUERY_KEYFILE=$DBT_GOOGLE_BIGQUERY_KEYFILE
    export SOA_DBT_GOOGLE_PROJECT=$DBT_GOOGLE_PROJECT
    export SOA_DBT_GOOGLE_BIGQUERY_DATASET_DEV=$DBT_GOOGLE_BIGQUERY_DATASET_DEV
    export SOA_DBT_GOOGLE_BIGQUERY_DATASET_PROD=$DBT_GOOGLE_BIGQUERY_DATASET_PROD
    export SOA_DBT_GOOGLE_BIGQUERY_DATASET_TEST=$DBT_GOOGLE_BIGQUERY_DATASET_TEST\n"
}
