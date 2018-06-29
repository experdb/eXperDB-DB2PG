package com.k4m.experdb.db2pg.convert.table.key.option;

public enum ForeignKeyMatch {
	FULL("MATCH FULL"), PARTIAL("MATCH PARTIAL"), SIMPLE("MATCH SIMPLE");
	final private String type;
	
	
	private ForeignKeyMatch(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}

}