package com.k4m.experdb.db2pg.convert.table.key;

public class Cluster extends Key<Cluster>{
	
	private String indexName;
	
	public Cluster() {
		super();
		type = Key.Type.CLUSTER;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	
}
