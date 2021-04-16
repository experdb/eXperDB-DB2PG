package com.k4m.experdb.db2pg.convert.table;

public class CustomSql {
	private String name;
	private String query;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	@Override
	public String toString() {
		return "[name=" + name + ", query=" + query + "]";
	}
}
