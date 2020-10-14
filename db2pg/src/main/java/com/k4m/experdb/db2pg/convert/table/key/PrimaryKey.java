package com.k4m.experdb.db2pg.convert.table.key;

import java.util.ArrayList;



public class PrimaryKey extends Key<PrimaryKey> { 
	
	public PrimaryKey() {
		super();
		type = Key.Type.PRIMARY;
	}
	
	public PrimaryKey(String tableSchema, String table,String keySchema, String name, ArrayList<String> columns, String indexName) {
		super(tableSchema, table,keySchema,name,columns,indexName);
		type = Key.Type.PRIMARY;
	}

	@Override
	public String toString() {
		return "Primary" + super.toString();
	}

}
