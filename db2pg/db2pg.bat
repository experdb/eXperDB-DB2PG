echo "DB2PG bat start run .. "
java -Dfile.encoding=UTF-8 -cp "%cd%\lib\*"  com.k4m.experdb.db2pg.Main $*
@echo on
