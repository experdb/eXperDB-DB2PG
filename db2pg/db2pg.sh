#!/bin/bash
echo "DB2PG start run .. "

SCRIPTPATH=$(cd "$(dirname "$0")" && pwd)
PROJECT_HOME=${SCRIPTPATH%/*}/db2pg
JAVA_HOME=$PROJECT_HOME/java/openjdk-7
LIB=$PROJECT_HOME/lib/*
JAVA_CLASSPATH=$PROJECT_HOME/lib/db2pg-2.0.0.jar
MAIN_CLASS=com.k4m.experdb.db2pg.Main

$JAVA_HOME/bin/java -Dfile.encoding=UTF-8 -cp .:lib/*  com.k4m.experdb.db2pg.Main $*