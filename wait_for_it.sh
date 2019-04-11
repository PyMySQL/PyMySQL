#!/usr/bin/env bash
# wait-for-it.sh

set -e

until mysqladmin -uroot status -h db; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 5
done

>&2 echo "Postgres is up - executing command"
exec
