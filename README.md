## eXperDB-DB2PG: Data Migration tool for PostgreSQL

## Introduction
eXperDB-DB2PG is a data migration solution that transfers data from various source DBMSs to eXperDB or PostgreSQL.
It works on JAVA basis, so there is no restriction on platforms such as Unix, Linux and Windows, and installation is not necessary and can be used easily.


![Architecture](./images/DB2PG_001_Architecture.png "eXperDB-DB2PG Architecture")


## Features
* Export full data or using WHERE clause.
* Export Oracle Spatial data to PostGIS.
* Export Oracle CLOB, BLOB object to PostgreSQL BYTEA.
* Support for any platform such as Linux and Windows.
* Faster than PostgreSQL COPY function.
* Removing FK and INDEX before performing data import operation(Rebuild after termination).
* Data export using select query is supported.
* Selective extraction through exclusion table.
* Support for Oracle, Oracle Spatial, SQL Server, Sybase.


## TODO
* Export Oracle schema to a PostgreSQL schema.
* Export DDL to PostgreSQL DDL.
* Support CUBRID.
<!--* Export predefined functions, triggers, procedures. -->
<!--* Support cubrid.-->


## License
[![LICENSE](https://img.shields.io/badge/LICENSE-GPLv3-ff69b4.svg)](https://github.com/experdb/eXperDB-Management/blob/master/LICENSE)


## Execution
#### 1. System Requirements
* OS : Developed and tested on Linux and Windows, but work on any UNIX-like system and Windows System
* JDK : JDK 1.8 or later
* Storage : With the SAM(Sequential Access Method) file, you need to enough free space to store the original data when transferring data.
* From : Oracle(Including Oracle Spatial), SQL Server, MySQL, Sybase
* To : eXperDB, PostgreSQL


#### 2. Options
|Option|Description|Mandatory|
|----------|--------|:----:|
|-c, --config `<arg>`|Configuration file load for executing DB2PG command. _(In the current version(1.1.2), the environment setting is not input as a parameter)_|Y|
|-M, --make-templates|Create a configuration file to run the DB2PG command|N|
|--rebuild-summary `<arg>`|Summarizes constraint execution logs such as PK, FK, INDEX created and deleted in Target Database. _(Create a file using '>' or '>>')_|N|
|--unload-summary `<arg>`|Summarize the logs loaded on the target using the Import.sql script generated from the Source Database. _(Create a file using '>' or '>>')_|N|


#### 3. Examples(On Linux)
|Command|E.g. use|
|-|-|
|Help|./db2pg.sh|
|Execute|./db2pg.sh -c db2pg.config|
|Create Import Log file|psql -U db2pg -d db2pg -p 5432 -h 127.0.0.1 -f import.sql  > import.log|
|Import Log Summary|./db2pg.sh --unload-summary ./db2pg-result/import.log                                        |
|Create Rebuild log file|psql -U db2pg -d db2pg -p 5432 -h 127.0.0.1 -f fk_drop.sql > rebuild.log|
||psql -U db2pg -d db2pg -p 5432 -h 127.0.0.1 -f idx_drop.sql >> rebuild.log|
||psql -U db2pg -d db2pg -p 5432 -h 127.0.0.1 -f idx_create.sql >> rebuild.log|
||psql -U db2pg -d db2pg -p 5432 -h 127.0.0.1 -f fk_create.sql >> rebuild.log|
|Rebuild Log Summary|bash db2pg.sh --rebuild-summary ./db2pg-result/rebuild/rebuild.log|


## Copyright
Copyright (c) 2016-2019, eXperDB Development Team All rights reserved.


## Community
* https://www.facebook.com/experdb
* http://cafe.naver.com/psqlmaster
