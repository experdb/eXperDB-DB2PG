#!/bin/bash

JAVA_HOME=$PWD/openjdk-8
MAIN_CLASS=com.k4m.experdb.db2pg.Main
java=$JAVA_HOME/bin/java

if [ "-version" == "$1" ]; then
        echo "DB2PG 64bit version \"2.2.10\""
        $java -version
else
        echo "DB2PG shell start run .. "
        $java -Dfile.encoding=UTF-8 -Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl -cp .:lib/* $MAIN_CLASS $*
fi