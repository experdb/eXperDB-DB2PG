package com.k4m.experdb.db2pg.convert.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.pattern.SqlPattern;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.table.key.Key;
import com.k4m.experdb.db2pg.convert.type.DDL_TYPE;
import com.k4m.experdb.db2pg.convert.vo.ConvertVO;
import com.k4m.experdb.db2pg.db.datastructure.exception.DBTypeNotFoundException;

public class TableStringParser extends StringParser<Table> {
	private ConvertMapper<?> convertMapper;
	private DDL_TYPE ddlType;
	
	public TableStringParser(ConvertMapper<?> convertMapper) {
		this.convertMapper = convertMapper;
	}
	
	@Override
	public Table parse(String ddlString, String compareValue) throws DBTypeNotFoundException {
		for (ConvertVO convertVO : convertMapper.getDefaultList()) {
			ddlString = ddlString.replace(convertVO.getAsValue(),convertVO.getToValue()).trim();
		}
		Table table = getTable(ddlString, compareValue);
		
		return table;
	}
	
	public DDL_TYPE getDDLType() {
		return ddlType;
	}

	private Table getTable (String ddlString, String compareValue) throws DBTypeNotFoundException {
		switch(compareValue) {
		case Constant.DB_TYPE.MYSQL : 
			return getTableMysqlToPog(ddlString, compareValue);
		default :
			throw new DBTypeNotFoundException();
		}
	}
	
	private Table getTableMysqlToPog (String ddlString, String compareValue) throws DBTypeNotFoundException {
		Table table = new Table();
		if(SqlPattern.MYSQL.CREATE_TABLE.matcher(ddlString).find()) {
			ddlType = DDL_TYPE.CREATE;
//			ddlString = stringConvertClassify(ddlString.replace("\r", ""),"U", "\"","'");
			
			int start = -1, end = -1;
			
			//Table Name Get
			String preString = ddlString.substring(0,ddlString.indexOf('('));
			preString = preString.replaceAll(SqlPattern.MYSQL.CREATE_TABLE.pattern(),"").trim().replace("\"","");
			if((start =preString.indexOf('.')) != -1) {
				table.setSchemaName(preString.substring(0, start));
				table.setName(preString.substring(start+1, preString.length()));
			} else {
				table.setName(preString);
			}
			
			//Table Comment
			String tailString = ddlString.substring(ddlString.lastIndexOf(')')+1);
			if(SqlPattern.check(tailString, SqlPattern.MYSQL.COMMENT)) {
				start = tailString.indexOf("'")+1;
				end = tailString.indexOf("'",start+1);
				table.setComment(tailString.substring(start, end));
			}
			
			//Table AutoIncrement
			if(SqlPattern.check(tailString, SqlPattern.MYSQL.AUTO_INCREMENT)) {
				Matcher mat = SqlPattern.MYSQL.AUTO_INCREMENT2.matcher(tailString);
				if(mat.find()) {
					try {
						int autoIncrement = Integer.parseInt(tailString.substring(mat.start(),mat.end()).replace("=", " ").split(" ")[1]);
						table.setAutoIncrement(autoIncrement);
					} catch (NumberFormatException nfe) {
						throw new NumberFormatException("AUTO_INCREMENT_PARSE_ERROR");
					}
				}
			}
			
			//Table Body
			start = ddlString.indexOf('(');
			end = ddlString.lastIndexOf(')');
			ddlString = ddlString.substring(start+1,end).trim().replace("\t", "").replace("\n", " ");
			List<String> tableStrs = ddlStringDivide(ddlString);
			StringParser<Key<?>> keyParser = new KeyParser(convertMapper);
			for(String tableStr : tableStrs) {
				if(SqlPattern.check(tableStr,SqlPattern.MYSQL.CONSTRAINT)
						|| SqlPattern.check(tableStr,SqlPattern.MYSQL.FULLTEXT)
						|| SqlPattern.check(tableStr,SqlPattern.MYSQL.KEY) 
						) {
					Key<?> key = keyParser.parse(tableStr, compareValue);
					if(key != null) {
						key.setTableName(table.getName());
						table.getKeys().add(key);
					}
					
				} else { // column
					StringParser<Column> columnParser = new ColumnParser(convertMapper);
					Column column = columnParser.parse(tableStr.trim(), compareValue);
					if( column != null) {
						table.getColumns().add(column);
					}
				}
			}
		} else {
			ddlType = DDL_TYPE.UNKNOWN;
		}
		
		return table;
	}
	
	
	
	private List<String> ddlStringDivide(String string) {
		string = string.replaceAll("\\s+", " ");
		List<String> strs = new ArrayList<String>();
		int start = -1, end = -1 , prevEnd = end;
		while((start = start == -1 ? 0 : string.indexOf(",",end)+1) <= (end = string.indexOf(",",start)) && prevEnd <= end) {
			String tmp = string.substring(start,end);
			while(tmp.contains("(") && !tmp.contains(")")) {
				end = string.indexOf(",",end+1);
				tmp = string.substring(start,end);
			}
			strs.add(string.substring(start,end));
			prevEnd = end;
		}
		strs.add(string.substring(prevEnd+1));
		return strs;
	}
	
	
}

