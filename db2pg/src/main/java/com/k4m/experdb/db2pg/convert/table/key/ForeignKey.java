package com.k4m.experdb.db2pg.convert.table.key;

import java.util.ArrayList;
import java.util.List;

import com.k4m.experdb.db2pg.convert.table.key.option.ReferenceDefinition;


public class ForeignKey extends Key<ForeignKey> {
	private String refTable;
	private String refTableSchema;
	private List<String> refColumns;
	private ReferenceDefinition refDef;
	
	public ForeignKey() {
		super();
		type = Key.Type.FOREIGN;
	}
	public ForeignKey(String tableSchema, String table,String keySchema, String name, ArrayList<String> columns,String refTable, String refTableSchema, ArrayList<String> refColumns) {
		super(tableSchema, table,keySchema ,name,columns);
		this.refTableSchema = refTableSchema;
		this.refTable = refTable;
		this.refColumns = refColumns;
		type = Key.Type.FOREIGN;
	}
	public String getRefTable() {
		return refTable;
	}
	public void setRefTable(String refTable) {
		this.refTable = refTable;
	}
	public String getRefTableSchema() {
		return refTableSchema;
	}
	public void setRefTableSchema(String refTableSchema) {
		this.refTableSchema = refTableSchema;
	}
	public List<String> getRefColumns() {
		return refColumns;
	}
	public void setRefColumns(List<String> refColumns) {
		this.refColumns = refColumns;
	}
	public ReferenceDefinition getRefDef() {
		return refDef;
	}
	public void setRefDef(ReferenceDefinition refDef) {
		this.refDef = refDef;
	}
	
	
	public boolean isSameKey(String tableSchema, String tableName, String keySchema, String keyName,String refTableSchema,String refTable) {
		if (!this.refTableSchema.equals(refTableSchema)) return false;
		if (!this.refTable.equals(refTable)) return false;
		return super.isSameKey(tableSchema, tableName, keySchema, keyName);
	}
	@Override
	public String toString() {
		return "ForeignKey [ refTable=" + refTable + " refColumns=" + refColumns + " refDef=" + refDef
				+ " keyName=" + getName() + " tableName=" + getTableName() + " columns="
				+ getColumns() + " indexType=" + (getIndexType()!=null?getIndexType().name():null) 
				+ " type=" + (getType()!=null?getType().name():null) + " ]";
	}
	
	
	
}
