package com.k4m.experdb.db2pg.convert.vo;


import java.util.List;

import com.k4m.experdb.db2pg.convert.type.COMMAND_TYPE;
import com.k4m.experdb.db2pg.convert.type.DDL_TYPE;

public class DDLStringVO {
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
	public DDLStringVO setString(String string) {
		this.string = string;
		return this;
	}
	public String getComment() {
		return comment;
	}
	public DDLStringVO setComment(String comment) {
		this.comment = comment;
		return this;
	}
	
	public List<String> getAlertComments() {
		return alertComments;
	}
	public DDLStringVO setAlertComments(List<String> alertComments) {
		this.alertComments = alertComments;
		return this;
	}
	public DDL_TYPE getDDLType() {
		return ddlType;
	}
	public DDLStringVO setDDLType(DDL_TYPE ddlType) {
		this.ddlType = ddlType;
		return this;
	}
	public Integer getPriority() {
		return priority;
	}
	public DDLStringVO setPriority(Integer priority) {
		this.priority = priority;
		return this;
	}
	public COMMAND_TYPE getCommandType() {
		return commandType;
	}
	public DDLStringVO setCommandType(COMMAND_TYPE commandType) {
		this.commandType = commandType;
		return this;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DDLStringVO) {
			DDLStringVO another = (DDLStringVO) obj;
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
	
	private static class Comparator implements java.util.Comparator<DDLStringVO> {
		@Override
		public int compare(DDLStringVO o1, DDLStringVO o2) {
			return o1.getPriority().compareTo(o2.getPriority());
		}
	}

}
