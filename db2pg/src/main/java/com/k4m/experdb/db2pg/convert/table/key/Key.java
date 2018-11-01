package com.k4m.experdb.db2pg.convert.table.key;

import java.util.ArrayList;
import java.util.List;

import com.k4m.experdb.db2pg.convert.table.key.exception.TableKeyException;


public abstract class Key<T> {
	protected List<Integer> ordinalPositions;
	protected String keyName;
	protected String keySchema;
	protected String tableSchema;
	protected String tableName;
	protected List<String> columns;
	protected IndexType indexType;
	protected Type type;
	protected String deferrable;
	protected String deferred;
	
	protected Key() {
	}
	
	protected Key(String tableSchema, String table,String keySchema, String name, ArrayList<String> columns) {
		this.tableSchema = tableSchema;
		this.keySchema = keySchema;
		this.tableName = table;
		this.keyName = name;
		this.columns = columns;
	}
	
	public List<Integer> getOrdinalPositions() {
		return ordinalPositions;
	}
	
	
	public void setOrdinalPositions(List<Integer> ordinalPositions) {
		this.ordinalPositions = ordinalPositions;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}
	
	public String getKeySchema() {
		return keySchema;
	}

	public void setKeySchema(String keySchema) {
		this.keySchema = keySchema;
	}

	public String getName() {
		return keyName;
	}
	public void setName(String keyName) {
		this.keyName = keyName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public IndexType getIndexType() {
		return indexType;
	}
	public void setIndexType(IndexType type) {
		this.indexType = type;
	}
	public Type getType() {
		return type;
	}
	public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public String getDeferrable() {
		return deferrable;
	}

	public void setDeferrable(String deferrable) {
		this.deferrable = deferrable;
	}

	public String getDeferred() {
		return deferred;
	}

	public void setDeferred(String deferred) {
		this.deferred = deferred;
	}

	public void setType(Type type) {
		this.type = type;
	}

	
	@SuppressWarnings("hiding")
	public <T> T unwrap(Class<T> iface) throws TableKeyException {
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new TableKeyException("Cannot unwrap to " + iface.getName());
	}
	public boolean isWrapperFor(Class<?> iface) {
		return iface.isAssignableFrom(getClass());
	}
	

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Key [");
		if(keySchema!=null) {
			sb.append(" keySchema=");
			sb.append(keySchema);
		}
		if(keyName!=null) {
			sb.append(" keyName=");
			sb.append(keyName);
		}
		if(tableSchema!=null) {
			sb.append(" tableSchema=");
			sb.append(tableSchema);
		}
		if(tableName!=null) {
			sb.append(" tableName=");
			sb.append(tableName);
		}
		if(columns!=null) {
			sb.append(" columns=");
			sb.append(columns);
		}
		if(type!=null) {
			sb.append(" type=");
			sb.append(type);
		}
		if(indexType!=null) {
			sb.append(" indexType=");
			sb.append(indexType); 
		}
			
		sb.append(" ]");
		return sb.toString();
	}

	public enum Type {
		NORMAL, PRIMARY, UNIQUE, FOREIGN, CLUSTER, VIEW
	}

	public enum IndexType {
		HASH, BTREE, NORMAL, BITMAP, FUNCTION_BASED_NORMAL, FUNCTION_BASED_BITMAP, DOMAIN
	}
	
	public boolean isSameKey(String tableSchema, String tableName, String keySchema, String keyName) {
		if (!this.tableSchema.equals(tableSchema)) return false;
		if (!this.tableName.equals(tableName)) return false;
		if (!this.keySchema.equals(keySchema)) return false;
		if (!this.keyName.equals(keyName)) return false;
		return true;
	}
	
}
