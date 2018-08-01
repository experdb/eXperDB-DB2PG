package com.k4m.experdb.db2pg.convert;


import java.util.List;

import com.k4m.experdb.db2pg.convert.type.COMMAND_TYPE;
import com.k4m.experdb.db2pg.convert.type.DDL_TYPE;

public class DDLString {
	private String string;
	private String comment;
	private List<String> alertComments;
	private DDL_TYPE ddlType;
	private int priority;
	private COMMAND_TYPE commandType;
	private static Comparator comparator;
	
	public String getString() {
		return string;
	}
	public DDLString setString(String string) {
		this.string = string;
		return this;
	}
	public String getComment() {
		return comment;
	}
	public DDLString setComment(String comment) {
		this.comment = comment;
		return this;
	}
	
	public List<String> getAlertComments() {
		return alertComments;
	}
	public DDLString setAlertComments(List<String> alertComments) {
		this.alertComments = alertComments;
		return this;
	}
	public DDL_TYPE getDDLType() {
		return ddlType;
	}
	public DDLString setDDLType(DDL_TYPE ddlType) {
		this.ddlType = ddlType;
		return this;
	}
	public Integer getPriority() {
		return priority;
	}
	public DDLString setPriority(Integer priority) {
		this.priority = priority;
		return this;
	}
	public COMMAND_TYPE getCommandType() {
		return commandType;
	}
	public DDLString setCommandType(COMMAND_TYPE commandType) {
		this.commandType = commandType;
		return this;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DDLString) {
			DDLString another = (DDLString) obj;
			return this.string.equals(another.string);
		}
		return false;
	}
	
	
	@Override
	public String toString() {
		return string+";";
	}
	
	public static Comparator getComparator() {
		if (comparator == null) {
			comparator = new Comparator();
		}
		return comparator;
	}
	
	private static class Comparator implements java.util.Comparator<DDLString> {
		@Override
		public int compare(DDLString o1, DDLString o2) {
			return o1.getPriority().compareTo(o2.getPriority());
		}
	}

}
