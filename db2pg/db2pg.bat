@echo off
java -Dfile.encoding=UTF-8 -cp ".;lib\*.jar;db2pg-2.0.0.jar" com.k4m.experdb.db2pg.Main %*
@echo on
