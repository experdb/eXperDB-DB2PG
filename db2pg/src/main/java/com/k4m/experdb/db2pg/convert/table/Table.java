package com.k4m.experdb.db2pg.convert.table;

import java.util.LinkedList;
import java.util.List;

import com.k4m.experdb.db2pg.convert.table.key.Key;

public class Table {
	private String schemaName;
	private String tableName;
	private String comment;
	private Integer ptCnt;
	private String ptType;
	private String ptSubType;
	private Integer ptSubCnt;
	private String partKeyColumn;
	private String partSubKeyColumn;
 
	private List<String> alertComments;
	private List<Column> columns;
	private List<Column> partColumns;
	private List<Column> subPartColumns;
	private List<Sequence> sequence;
	private List<Key<?>> keys;
	private List<View> views;
	private boolean checkColumn;
	private boolean hasGeometry;

/*    PT.PARTITIONING_TYPE AS PT_TYPE,
    PT.SUBPARTITIONING_TYPE AS PT_SUB_TYPE,
    PT.PARTITION_COUNT AS PT_CNT,
    PT.DEF_SUBPARTITION_COUNT AS PT_SUB_CNT,
    PT.PARTITIONING_KEY_COUNT AS PT_KEY_CNT,
    PT.SUBPARTITIONING_KEY_COUNT AS PT_SUB_KEY_CNT*/
    
	public Table() {
		this.views = new LinkedList<View>();
		this.columns = new LinkedList<Column>();
		this.partColumns = new LinkedList<Column>();
		this.subPartColumns = new LinkedList<Column>();
		this.sequence = new LinkedList<Sequence>();
		this.keys = new LinkedList<Key<?>>();
		this.alertComments = new LinkedList<String>();
		this.checkColumn = false;
		this.hasGeometry = false;
	}
	
	
	public boolean isHasGeometry() {
		return hasGeometry;
	}


	public void setHasGeometry(boolean hasGeometry) {
		this.hasGeometry = hasGeometry;
	}


	public String getPartSubKeyColumn() {
		return partSubKeyColumn;
	}


	public void setPartSubKeyColumn(String partSubKeyColumn) {
		this.partSubKeyColumn = partSubKeyColumn;
	}


	public String getPartKeyColumn() {
		return partKeyColumn;
	}


	public void setPartKeyColumn(String partKeyColumn) {
		this.partKeyColumn = partKeyColumn;
	}


	public int getPtCnt() {
		return ptCnt;
	}

	public void setPtCnt(int ptCnt) {
		this.ptCnt = ptCnt;
	}

	public String getPtType() {
		return ptType;
	}

	public void setPtType(String ptType) {
		this.ptType = ptType;
	}

	public String getPtSubType() {
		return ptSubType;
	}

	public void setPtSubType(String ptSubType) {
		this.ptSubType = ptSubType;
	}

	public int getPtSubCnt() {
		return ptSubCnt;
	}

	public void setPtSubCnt(int ptSubCnt) {
		this.ptSubCnt = ptSubCnt;
	}

	public boolean isCheckColumn() {
		return checkColumn;
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

	public List<Sequence> getSequence() {
		return sequence;
	}
	
	public List<Column> getColumns() {
		return columns;
	}
	
	public List<Column> getPartColumns() {
		return partColumns;
	}
	
	public List<Column> getSubPartColumns() {
		return subPartColumns;
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
	
	
	public void setCheckColumn(boolean checkColumn) {
		this.checkColumn = checkColumn;
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
		
		if (sequence != null) {
			sb.append(" sequence=");
			sb.append(sequence);
		}
		sb.append(" ]");
		return sb.toString();
	}

}
