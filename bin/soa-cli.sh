#!/bin/bash

set -o pipefail
[ -z "$DEBUG" ] || set -x

BIN_DIR=$(dirname $0)
export PATH=$BIN_DIR:$PATH

source $BIN_DIR/.gcp-vars.sh
libs="gcp-login gcp-connect gcp-utils"
for l in $( echo "$libs" ) ; do
  source "$BIN_DIR/.$l-lib.sh"
done

function main {
  cmd="$1"

  if [[ "$cmd" == "help" ]] || [[ -z "$cmd" ]] || [[ ! "$(declare -F "$cmd")" ]]; then
    echo "usage:
    $(basename $0) <command>
    $(basename $0) <command> help

    login - login into GCP under a Google user account and set default credentials/project
    connect - connect through IAP tunnel w/ bastion host to the service on GCP (airbyte, dmt-sql)
    disconnect - close ssh connection/tunnel to the service on GCP (airbyte, dmt-sql)
    "
    exit 1
  fi

  shift
  $cmd "$@"
}

main "$@"