#!/bin/bash

function upcase-with-underscore {
  echo "$(echo "$1" | tr '[:lower:]' '[:upper:]' | tr '-' '_')"
}

function print_usage {
  echo "Usage of $1: "
  case "$1" in
    disconnect)
      cat <<-EOF
      $(basename $0) $1 <service>
        -- <service> must be either 'airbyte' or 'dmt-sql-<dev|prod>'
EOF
    ;;
    connect)
      cat <<-EOF
      $(basename $0) $1 <service> [port]
        -- <service> must be either 'airbyte' or 'dmt-sql-<dev|prod>'
        -- [port] local port used for forwarding; if none is passed defaults are used
          'airbyte' default: 8080
          'dmt-sql-dev' default: 5435
          'dmt-sql-prod' default: 5436
EOF
    ;;
    login)
      echo "Login" ;;
    *) ;;
  esac
  exit 1
}