#!/bin/sh
# wait-for-postgres.sh

set -e

hostpg="$1"
hostora="$2"
shift 2
cmd="$@"

#until PGPASSWORD=$POSTGRES_PASSWORD /usr/bin/psql -h "${hostpg}" -U "postgres" hr -c 'select count(*) from hr.employees'; do
#  >&2 echo "schema hr in ${hostpg} is absent - sleeping 15s"
#  sleep 15
#done
#>&2 echo "Postgres is up and hr exists"


cat >/tmp/check_hr.sql <<EOF
SET FEED OFF
SET VERIFY OFF
CONNECT &1/&2@'(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=&3)(PORT=1521))(CONNECT_DATA=(SID=XE)(SERVER=DEDICATED))))'
COLUMN tab_count NEW_VALUE table_count
SELECT CASE WHEN COUNT(*)=0 THEN 1 ELSE 0 END tab_count FROM hr.departments;
EXIT table_count
EOF

until USER=$ORACLE_USER;PWD=$ORACLE_PASSWORD;HOST=$hostora;sqlplus -s -l /nolog @/tmp/check_hr.sql $USER $PWD $HOST; do
  >&2 echo "schema hr in ${hostora} is absent - sleeping 15s"
  sleep 15
done

>&2 echo "Oracle is up and hr exists"

echo "executing [$cmd]"
exec $cmd

