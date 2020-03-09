#!/bin/bash

. oraenv 

echo "Create user HR"
sqlplus -S -L / as sysdba <<EOF
ALTER user system identified by ${ORACLE_PASSWORD};
EOF
sqlplus -S -L / as sysdba @${ORACLE_BASE}/admin/XE/dpdump/${SQLINIT}
