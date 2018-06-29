package com.k4m.experdb.db2pg.convert.table;


public class Column {
	private static Comparator comparator;
	private Integer ordinalPosition;
	private String columnName;
	private String comment;
	private String defaultValue;
	private String type;
	private String extra;
	private Integer numericPrecision;
	private Integer numericScale;
	private boolean isNotNull;
	private boolean isAutoIncreament;
	public Integer getOrdinalPosition() {
		return ordinalPosition;
	}
	public void setOrdinalPosition(Integer ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}
	public String getName() {
		return columnName;
	}
	public void setName(String columnName) {
		this.columnName = columnName;
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public boolean isNotNull() {
		return isNotNull;
	}
	public void setNotNull(boolean isNotNull) {
		this.isNotNull = isNotNull;
	}
	public boolean isAutoIncreament() {
		return isAutoIncreament;
	}
	public void setAutoIncreament(boolean isAutoIncreament) {
		this.isAutoIncreament = isAutoIncreament;
	}
	public String getExtra() {
		return extra;
	}
	public void setExtra(String extra) {
		this.extra = extra;
	}
	public Integer getNumericPrecision() {
		return numericPrecision;
	}
	public void setNumericPrecision(Integer numeric_precision) {
		this.numericPrecision = numeric_precision;
	}
	public Integer getNumericScale() {
		return numericScale;
	}
	public void setNumericScale(Integer numeric_scale) {
		this.numericScale = numeric_scale;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(" Column[");
		if(columnName != null) {
			sb.append(" columnName=");
			sb.append(columnName);
		}
		if(comment != null) {
			sb.append(" comment=");
			sb.append(comment);
		}
		if(defaultValue != null) {
			sb.append(" defaultValue=");
			sb.append(defaultValue);
		}
		if(type != null) {
			sb.append(" type=");
			sb.append(type);
		}
		sb.append(" isNotNull=");
		sb.append(isNotNull);
		sb.append(" isAutoIncreament=");
		sb.append(isAutoIncreament);
		sb.append(" ]");
		return sb.toString();
	}
	public static Comparator getComparator() {
		if (comparator == null) {
			comparator = new Comparator();
		}
		return comparator;
	}
	
	private static class Comparator implements java.util.Comparator<Column> {
		@Override
		public int compare(Column o1, Column o2) {
			return o1.getOrdinalPosition().compareTo(o2.getOrdinalPosition());
		}
	}
}