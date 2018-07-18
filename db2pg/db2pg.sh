#!/bin/bash
java -Dfile.encoding=UTF-8 -cp .:lib/*.jar:db2pg-1.1.3.jar  com.k4m.experdb.db2pg.Main $*