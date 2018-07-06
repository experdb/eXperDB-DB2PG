## eXperDB-DB2PG: Data Migration tool for PostgreSQL

## Introduction
eXperDB-DB2PG는 다양한 원천 DBMS로부터 eXperDB 또는 PostgreSQL로 데이터를 이관하는 데이터 이관 솔루션이다.
JAVA 기반으로 동작하여 Unix, Linux 및 Windows 등 플랫폼에 대한 제약이 없고, 설치가 필요하지 않아 간편하게 활용할 수 있다.

eXperDB-DB2PG is a data migration solution that transfers data from various source DBMSs to eXperDB or PostgreSQL.
It works on JAVA basis, so there is no restriction on platforms such as Unix, Linux and Windows, and installation is not necessary and can be used easily.


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
* Export predefined functions, triggers, procedures.
* Support mysql(mariaDB)
<!--* Support cubrid.-->


## License
[![LICENSE](https://img.shields.io/badge/LICENSE-GPLv3-ff69b4.svg)](https://github.com/experdb/eXperDB-Management/blob/master/LICENSE)


## Copyright
Copyright (c) 2016-2018, eXperDB Development Team All rights reserved.


## Community
* https://www.facebook.com/experdb
* http://cafe.naver.com/psqlmaster
