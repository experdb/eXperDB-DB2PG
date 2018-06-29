package com.k4m.experdb.db2pg.convert.table.key;

import java.util.ArrayList;

import com.k4m.experdb.db2pg.convert.table.Column;


public class UniqueKey extends Key<UniqueKey> {
	public UniqueKey() {
		super();
		type = Key.Type.UNIQUE;
	}
	
	public UniqueKey(String tableSchema, String table,String keySchema, String name, ArrayList<String> columns) {
		super(tableSchema, table,keySchema,name,columns);
		type = Key.Type.UNIQUE;
	}

	@Override
	public String toString() {
		return "Unique"+super.toString();
	}
	
	
	
}
