#!/bin/sh
# wait-for-postgres.sh

set -e

hostpg="$1"
hostora="$2"
shift 2
cmd="$@"

until PGPASSWORD=$POSTGRES_PASSWORD /usr/bin/psql -h "${hostpg}" -U "postgres" hr -c 'select count(*) from hr.employees'; do
  >&2 echo "schema hr in ${hostpg} is absent - sleeping 15s"
  sleep 15
done
>&2 echo "Postgres is up and hr exists"

until USER=$ORACLE_USER;PWD=$ORACLE_PASSWORD;HOST=$hostora;sqlplus -s -l /nolog @check_hr.sql $USER $PWD $HOST; do
  >&2 echo "schema hr in ${hostora} is absent - sleeping 15s"
  sleep 15
done

>&2 echo "Oracle is up and hr exists"

echo "executing [$cmd]"
exec $cmd

