package com.k4m.experdb.db2pg.convert.table.key;

public class CLUSTER extends Key<CLUSTER>{
	
	private String index_name;
	
	public CLUSTER() {
		super();
		type = Key.Type.CLUSTER;
	}

	public String getIndex_name() {
		return index_name;
	}

	public void setIndex_name(String index_name) {
		this.index_name = index_name;
	}
	
}
