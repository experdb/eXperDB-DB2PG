package com.k4m.experdb.db2pg.convert.table;

import java.util.LinkedList;
import java.util.List;

import com.k4m.experdb.db2pg.convert.table.key.Key;

public class Table {
	private String schemaName;
	private String tableName;
	private String comment;
	private List<String> alertComments;
	private List<Column> columns;
	private List<Key<?>> keys;
	private List<View> views;

	public Table() {
		this.views = new LinkedList<View>();
		this.columns = new LinkedList<Column>();
		this.keys = new LinkedList<Key<?>>();
		this.alertComments = new LinkedList<String>();
	}

	public List<View> getViews() {
		return views;
	}

	public void setViews(List<View> views) {
		this.views = views;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getName() {
		return tableName;
	}

	public void setName(String tableName) {
		this.tableName = tableName;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public List<String> alertComments() {
		return this.alertComments;
	}

	public List<Key<?>> getKeys() {
		return keys;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Table) {
			Table another = (Table) obj;
			if (this.schemaName.equals(another.schemaName)) {
				if (this.tableName.equals(another.tableName)) {

				}
			}
		}
		return false;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Table [");
		if (schemaName != null) {
			sb.append(" schemaName=");
			sb.append(schemaName);
		}
		if (tableName != null) {
			sb.append(" tableName=");
			sb.append(tableName);
		}
		if (comment != null) {
			sb.append(" comment=");
			sb.append(comment);
		}
		if (columns != null) {
			sb.append(" columns=");
			sb.append(columns);
		}
		if (keys != null) {
			sb.append(" keys=");
			sb.append(keys);
		}
		if (views != null) {
			sb.append(" views=");
			sb.append(views);
		}
		sb.append(" ]");
		return sb.toString();
	}

}
