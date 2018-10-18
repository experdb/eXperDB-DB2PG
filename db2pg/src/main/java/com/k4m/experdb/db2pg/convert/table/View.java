package com.k4m.experdb.db2pg.convert.table;

public class View {
	private String tableCatalLog;
	private String tableSchema;
	private String tableName;
	private String viewDefinition;
	private String checkOption;
	private String isUpdaTable;

	public String getTableCatalLog() {
		return tableCatalLog;
	}

	public void setTableCatalLog(String tableCatalLog) {
		this.tableCatalLog = tableCatalLog;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getViewDefinition() {
		return viewDefinition;
	}

	public void setViewDefinition(String viewDefinition) {
		this.viewDefinition = viewDefinition;
	}

	public String getCheckOption() {
		return checkOption;
	}

	public void setCheckOption(String checkOption) {
		this.checkOption = checkOption;
	}

	public String getIsUpdaTable() {
		return isUpdaTable;
	}

	public void setIsUpdaTable(String isUpdaTable) {
		this.isUpdaTable = isUpdaTable;
	}

}
