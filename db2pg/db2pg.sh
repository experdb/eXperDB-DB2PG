#!/bin/bash

SCRIPTPATH=$(cd "$(dirname "$0")" && pwd)
PROJECT_HOME=${SCRIPTPATH%/*}/db2pg
JAVA_HOME=$PROJECT_HOME/java/openjdk-8
LIB=$PROJECT_HOME/lib/*
JAVA_CLASSPATH=$PROJECT_HOME/lib/db2pg-2.0.0.jar
MAIN_CLASS=com.k4m.experdb.db2pg.Main

if [ "-version" == "$1" ]; then
  echo "DB2PG 64bit version \"1.2.0\""
  echo "openjdk version \"1.8.0_141\""
else
	echo "DB2PG start run .. "
	$JAVA_HOME/bin/java -Dfile.encoding=UTF-8 -cp .:lib/*  com.k4m.experdb.db2pg.Main $*
fi
