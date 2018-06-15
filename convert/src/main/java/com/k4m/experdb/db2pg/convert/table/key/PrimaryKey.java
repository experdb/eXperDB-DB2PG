package com.k4m.experdb.db2pg.convert.table.key;

import java.util.ArrayList;

import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.key.exception.TableKeyException;


public class PrimaryKey extends Key<PrimaryKey> { 
	
	public PrimaryKey() {
		super();
		type = Key.Type.PRIMARY;
	}
	
	public PrimaryKey(String tableSchema, String table,String keySchema, String name, ArrayList<String> columns) {
		super(tableSchema, table,keySchema,name,columns);
		type = Key.Type.PRIMARY;
	}

	@Override
	public String toString() {
		return "Primary" + super.toString();
	}

}
