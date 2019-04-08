#!/bin/bash

MAIN_CLASS=com.k4m.experdb.db2pg.Main

if [ "-version" == "$1" ]; then
        echo "DB2PG 64bit version \"1.2.3\""
else
        echo "DB2PG start run .. "
        java -Dfile.encoding=UTF-8 -cp .:lib/* $MAIN_CLASS $*
fi
