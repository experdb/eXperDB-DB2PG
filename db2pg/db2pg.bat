echo "DB2PG bat start run .. "
java -Dfile.encoding=UTF-8 -Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl -cp "%cd%\lib\*"  com.k4m.experdb.db2pg.Main $*
@echo on
