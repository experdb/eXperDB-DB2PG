#!/bin/bash

echo "eXperDB-DB2PG SETUP"

DB2PG_HOME=$(cd "$(dirname "$0")" && pwd)
JAVA=$DB2PG_HOME/openjdk-8/bin/java

chmod 755 $DB2PG_HOME/db2pg.sh
sleep 3

chmod +x $JAVA
sleep 3

cd "${DB2PG_HOME}"
mkdir ddl config trans xml
