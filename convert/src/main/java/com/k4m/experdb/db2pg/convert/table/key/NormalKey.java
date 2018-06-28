package com.k4m.experdb.db2pg.convert.table.key;

import java.util.ArrayList;


public class NormalKey extends Key<NormalKey> {
	
	public NormalKey() {
		super();
		type = Key.Type.NORMAL;
	}
	
	public NormalKey(String tableSchema, String table,String keySchema, String name, ArrayList<String> columns) {
		super(tableSchema, table,keySchema,name,columns);
		type = Key.Type.NORMAL;
	}
}