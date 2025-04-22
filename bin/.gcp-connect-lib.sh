#!/bin/bash

function connect {
  [[ "$1" == "help" ]] && print_usage $FUNCNAME
  
  gcp_service=$1
  [[ "$gcp_service" =~ ^(airbyte|dmt-sql-dev|dmt-sql-prod|gc-intel-sql-dev)$ ]] || print_usage $FUNCNAME

  connection_name="$1-tunnel"
  connection_file_name="/tmp/.ssh-$1-tunnel"

  [[ -S "$connection_file_name" ]] && ssh -S $connection_file_name -O exit $connection_name 2>/dev/null
  
  bastion_port_var="$(upcase-with-underscore $gcp_service)_BASTION_PORT"
  local_port_var="$(upcase-with-underscore $gcp_service)_LOCAL_PORT_DEFAULT"
  
  bastion_port=$(eval "echo \"\$$bastion_port_var\"")
  local_port=$(eval "echo \"\$$local_port_var\"") && [[ ! -z $2 ]] && local_port=$2
  
  gcloud --verbosity=none --quiet compute ssh bastion-host-vm-dev \
    --zone=$GCP_DEFAULT_REGION_ZONE --tunnel-through-iap \
    -- -M -S $connection_file_name -L $local_port:localhost:$bastion_port -f -N $connection_name

  if [ "$1" == "dmt-sql-dev" ] || [ "$1" == "dmt-sql-prod" ] || [ "$1" == "gc-intel-sql-dev" ]; then
    postgres_user=$( \
      gcloud --verbosity=none --quiet compute ssh bastion-host-vm-dev \
        --zone=$GCP_DEFAULT_REGION_ZONE --tunnel-through-iap \
        -- gcloud --verbosity=none --quiet config get-value account | sed "s/.gserviceaccount.com//" \
    )
    postgres_password=$( \
      gcloud --verbosity=none --quiet compute ssh bastion-host-vm-dev \
        --zone=$GCP_DEFAULT_REGION_ZONE --tunnel-through-iap \
        -- gcloud --verbosity=none --quiet sql generate-login-token \
    )
    cat <<-EOF
Connection settings for Cloud SQL Postgres ($1):
  -- port: $local_port
  -- host: localhost
  -- user: $postgres_user
  -- password: $postgres_password
EOF
  fi
}

function disconnect {
  [[ "$1" == "help" ]] && print_usage $FUNCNAME
  
  gcp_service=$1
  [[ "$gcp_service" =~ ^(airbyte|dmt-sql-dev|dmt-sql-prod|gc-intel-sql-dev)$ ]] || print_usage $FUNCNAME

  connection_name="$1-tunnel"
  connection_file_name="/tmp/.ssh-$1-tunnel"

  [[ ! -S "$connection_file_name" ]] && echo "There's no active connection to $1" && exit 1
  ssh -S $connection_file_name -O exit $connection_name
}