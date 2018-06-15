# eXperDB-DB2PG

Any RDBMS to PostgreSQL Data Migrator

## Feature list
- Export full data or following a WHERE clause.
- Export views as PG tables.
- Export Oracle geometry data into PostGis.
- Export Oracle CLOB, BLOB object as PG BYTEA.
- Support for any platform such as Linux and window.
- Faster than PostgreSQL COPY function.
- Removing fk and index before performing data import operation(Rebuild after termination).
- Data export using select query is supported.
- Selective extraction through exclusion table.
- Support oracle, sysbase and ms-sql.

## TODO
- Export Oracle schema to a PostgreSQL schema.
- Export DDL to PostgreSQL DDL.
- Export predefined functions, triggers, procedures.
- Support cubrid.
- Support mysql(mariaDB)

# LICENSE
	Copyright (c) 2016-2018 K4M - All rights reserved.
		This program is free software: you can redistribute it and/or modify
		it under the terms of the GNU General Public License as published by
		the Free Software Foundation, either version 3 of the License, or
		any later version.
		This program is distributed in the hope that it will be useful,
		but WITHOUT ANY WARRANTY; without even the implied warranty of
		MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
		GNU General Public License for more details.
		You should have received a copy of the GNU General Public License
		along with this program.  If not, see < http://www.gnu.org/licenses/ >.
