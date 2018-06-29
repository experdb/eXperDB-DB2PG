package com.k4m.experdb.db2pg.convert.parse;


import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.pattern.SqlPattern;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.vo.ConvertVO;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.db.datastructure.exception.DBTypeNotFoundException;

public class ColumnParser extends StringParser<Column> {
	private ConvertMapper<?> convertMapper;

	public ColumnParser(ConvertMapper<?> convertMapper) {
		this.convertMapper = convertMapper;
	}

	@Override
	public Column parse(String ddlString, String compareValue) throws DBTypeNotFoundException {
		return getColumn(ddlString, compareValue);
	}
	private Column getColumn (String ddlString, String compareValue) throws DBTypeNotFoundException {
		switch(compareValue) {
		case Constant.DB_TYPE.MYSQL : 
			return getColumnMysqlToPog(ddlString);
		default :
			throw new DBTypeNotFoundException();
		}
	}
	private Column getColumnMysqlToPog (String ddlString) throws DBTypeNotFoundException {
		Column column = new Column();
		if(SqlPattern.MYSQL.NOT_NULL.matcher(ddlString).find()) {
			column.setNotNull(true);
			ddlString = ddlString.replaceAll(SqlPattern.MYSQL.NOT_NULL.pattern(), "");
		} else {
			column.setNotNull(false);
		}
		if(SqlPattern.MYSQL.NULL.matcher(ddlString).find()){
			ddlString = ddlString.replaceAll(SqlPattern.MYSQL.NULL.pattern(), "");
		}
		if(SqlPattern.MYSQL.AUTO_INCREMENT.matcher(ddlString).find()) {
			column.setAutoIncreament(true);
			ddlString = ddlString.replaceAll(SqlPattern.MYSQL.AUTO_INCREMENT.pattern(), "");
		} else {
			column.setAutoIncreament(false);
		}
		if(SqlPattern.MYSQL.DEFAULT.matcher(ddlString).find()) {
			String[] tmpStrs1 =  SqlPattern.MYSQL.DEFAULT.split(ddlString, 2);
			String[] tmpStrs2 = tmpStrs1[1].trim().split(" ");
			String defaultValue = tmpStrs2[0];
			StringBuilder sb = new StringBuilder(tmpStrs1[0]);
			for( int i=1; i<tmpStrs2.length; i++ ) {
				sb.append(" ");
				sb.append(tmpStrs2[i]);
			}
			column.setDefaultValue(defaultValue);
			ddlString = sb.toString();
		}
		if(SqlPattern.MYSQL.COMMENT.matcher(ddlString).find()) {
			String[] tmpStrs1 =  SqlPattern.MYSQL.COMMENT.split(ddlString, 2);
			String[] tmpStrs2 = tmpStrs1[1].trim().split(" ");
			String commandValue = tmpStrs2[0];
			StringBuilder sb = new StringBuilder(tmpStrs1[0]);
			for( int i=1; i<tmpStrs2.length; i++ ) {
				sb.append(" ");
				sb.append(tmpStrs2[i]);
			}
			column.setComment(commandValue.trim().replace("'", ""));
			ddlString = sb.toString();
		}
		String columnName = ddlString.substring(0,ddlString.indexOf(" ")).replace("\"", "");
		String typeName = ddlString.substring(ddlString.indexOf(" ")).trim();
		column.setName(columnName);
		
		column.setType(typeName);
		return column;
	}
}
