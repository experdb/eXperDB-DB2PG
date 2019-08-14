package com.k4m.experdb.db2pg.convert.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.table.View;
import com.k4m.experdb.db2pg.convert.table.key.ForeignKey;
import com.k4m.experdb.db2pg.convert.table.key.Key.IndexType;
import com.k4m.experdb.db2pg.convert.table.key.Key.Type;
import com.k4m.experdb.db2pg.convert.table.key.NormalKey;
import com.k4m.experdb.db2pg.convert.table.key.PrimaryKey;
import com.k4m.experdb.db2pg.convert.table.key.UniqueKey;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyDelete;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyMatch;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyUpdate;
import com.k4m.experdb.db2pg.convert.table.key.option.ReferenceDefinition;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.db.impl.WORK_TYPE;

public class MySQLConvertDBUtils {
	static MsgCode msgCode = new MsgCode();
	/** [Add] sequence extract function**/
	/**
	 * <br>schema's name : schema_name (string)
	 * <br>table's name : table_name (string)
	 * <br>table has comment : table_comment (string)
	 * */
	public static List<Table> getTableInform(List<String> tableNames,boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<Table> tables = new ArrayList<Table>();
		try {
			LogUtils.info(msgCode.getCode("C0031"),MySQLConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
//			String columnName = null;
//			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYS)) {
//				columnName = "table_name";
//			} else if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MSS)) {
//				columnName = "o.name";
//			}
			
			if(tableNames != null && !tableNames.isEmpty()) {
				StringBuilder sb = new StringBuilder();
//				sb.append("AND "+columnName+" IN (");
				for(String tableName : tableNames) {
					sb.append("'");
					sb.append(tableName);
					sb.append("',");
				}
				sb.deleteCharAt(sb.length()-1);
//				sb.append(")");
				params.put("TABLE_LIST", sb.toString());
			} else {
				params.put("TABLE_LIST", "");
			}
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_TABLE_INFORM, params));
			mew.run();
			List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
			LogUtils.info(msgCode.getCode("C0032")+results,MySQLConvertDBUtils.class);
			LogUtils.info(msgCode.getCode("C0033")+results,MySQLConvertDBUtils.class);
			Object obj = null;
			for (Map<String,Object> result : results) {
				Table table = new Table();
				obj = result.get("schema_name");
				if(obj!=null) table.setSchemaName(obj.toString());
				obj = result.get("table_name");
				if(obj!=null) table.setName(obj.toString());
				obj = result.get("table_comment");
				if(obj!=null) table.setComment(obj.toString());
				tables.add(table);
			}
		} catch(Exception e){
			LogUtils.error(e.getMessage(),MySQLConvertDBUtils.class);
		} finally {
			LogUtils.info(msgCode.getCode("C0034"),MySQLConvertDBUtils.class);
		}
		
		return tables;
	}

	
	/**
	 * <br>Column's ordinal position : ordinal_position (int) 
	 * <br>Column's name : column_name (string)
	 * <br>Column's type : column_type (string)
	 * <br>Column has default value : column_default (string)
	 * <br>Column has contain null : is_null (boolean) 
	 * <br>Column has comment : column_comment (string)
	 * <br>Any additional information that is available about a given column : extra (string)
	 * <br>Precision setting when the column data type is numeric : numeric_precision (int)
	 * <br>Scale setting when the column data type is numeric : numeric_scale (int)
	 * <br>Sequnce's start value : seq_start (long)
	 * <br>Sequnce's minimal value : seq_min_value (long)
	 * <br>Sequnce's increment value : seq_inc_value (long)
	 * */
	public static Table setColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info(msgCode.getCode("C0035"),MySQLConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("TABLE_SCHEMA", table.getSchemaName());
			params.put("TABLE_NAME", table.getName());
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_COLUMN_INFORM, params));
			mew.run();
			List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
			LogUtils.info(msgCode.getCode("C0036")+results,MySQLConvertDBUtils.class);
			Object obj = null;
        	for (Map<String,Object> result : results) {
        		Column column = new Column();
        		obj = result.get("ordinal_position");
        		if(obj != null) column.setOrdinalPosition(Integer.valueOf(obj.toString()));
        		obj = result.get("column_name");
        		column.setName(obj!=null?obj.toString():null);
        		obj = result.get("column_default");
        		column.setDefaultValue(obj!=null?obj.toString():null);
        		obj = result.get("is_null");
        		if(obj != null) column.setNotNull(!Boolean.valueOf(obj.toString()));
        		obj = result.get("numeric_precision");
        		if(obj != null) column.setNumericPrecision(Integer.valueOf(obj.toString()));
        		obj = result.get("numeric_scale");
        		if(obj != null) column.setNumericScale(Integer.valueOf(obj.toString()));
        		obj = result.get("column_type");
        		column.setType(obj!=null?obj.toString():null);
        		obj = result.get("column_comment");
        		column.setComment(obj!=null?obj.toString():null);
        		obj = result.get("seq_start");
        		if(obj != null) column.setSeqStart(Long.valueOf(obj.toString()));
        		obj = result.get("seq_min_value");
        		if(obj != null) column.setSeqMinValue(Long.valueOf(obj.toString()));
        		obj = result.get("seq_inc_value");
        		if(obj != null) column.setSeqIncValue(Long.valueOf(obj.toString()));
        		obj = result.get("extra");
        		column.setExtra(obj!=null?obj.toString():null);
        		table.getColumns().add(column);
        		
        	}
        	Collections.sort(table.getColumns(),Column.getComparator());
			LogUtils.info(msgCode.getCode("C0037"),MySQLConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),MySQLConvertDBUtils.class);
		}
		return table;
	}
	
	/**
	 * <br>Constraint's schema name : constraint_schema (string)
	 * <br>Constraint's name : constraint_name (string)
 	 * <br>Table's schema name : table_schema (string)
 	 * <br>Table's name : table_name (string)
	 * <br>Column's name : column_name (string)
	 * <br>Column's ordinal position : ordinal_position (int)
	 * <br>Referenced table's schema name :  ref_table_schema (string)
	 * <br>Referenced table's name :  ref_table_name (string)
	 * <br>Referenced Column's name : ref_column_name (string)
	 * <br>Constraint's type : constraint_type (enum)
	   <br>&nbsp - P : primary key
	   <br>&nbsp - U : unique key
	   <br>&nbsp - F : foreign key
	 * <br>Index's type : index_type (enum)
	   <br>&nbsp - HASH : hash index
	   <br>&nbsp - BTREE : btree index
	 * <br>Match option of the foreign key constraint : match_option (enum)
	   <br>&nbsp - FULL : MATCH FULL
	   <br>&nbsp - PARTIAL : PostgreSQL is not yet implemented. ( 2018.07.30 )
	   <br>&nbsp - SIMPLE : MATCH SIMPLE
	 * <br>ON UPDATE clause specifies the action to perform when a referenced column in the referenced table is being updated to a new value : update_rule (enum)
	   <br>&nbsp - NO ACTION
	   <br>&nbsp - RESTRICT
	   <br>&nbsp - CASCADE 
	   <br>&nbsp - SET NULL 
	   <br>&nbsp - SET DEFAULT 
	 * <br>ON DELETE clause specifies the action to perform when a referenced row in the referenced table is being deleted : delete_rule (enum)
	   <br>&nbsp - NO ACTION
	   <br>&nbsp - RESTRICT
	   <br>&nbsp - CASCADE 
	   <br>&nbsp - SET NULL 
	   <br>&nbsp - SET DEFAULT
	*/
	public static Table setConstraintInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info(msgCode.getCode("C0038"),MySQLConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYS)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
			}
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_CONSTRAINT_INFORM, params));
			mew.run();
			
			List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
			LogUtils.info(msgCode.getCode("C0039") + results, ConvertDBUtils.class);
			Object obj = null;
        	for(Map<String,Object> result : results ) {
        		
        		String constraintType = (obj=result.get("constraint_type")) != null ?obj.toString():null;
        		if(constraintType.equals("P")) {
        			String keySchema = (obj=result.get("constraint_schema")) != null ?obj.toString():null;
        			String keyName = (obj=result.get("constraint_name")) != null ?obj.toString():null;
        			String tableSchema = (obj=result.get("table_schema")) != null ?obj.toString():null;
        			String tableName = (obj=result.get("table_name")) != null ?obj.toString():null;
        			String columnName = (obj=result.get("column_name")) != null ?obj.toString():null;
        			obj = result.get("ordinal_position");
        			int ordinalPosition = -1;
        			if(obj!=null) ordinalPosition = Integer.valueOf(obj.toString());
        			String indexType = (obj=result.get("index_type")) != null ?obj.toString():null;
        			
        			PrimaryKey pkey = new PrimaryKey();
        			boolean isAdded = false;
        			for(int i=0; i<table.getKeys().size();i++) {
        				if(table.getKeys().get(i).getType().name().equals(Type.PRIMARY.name())) {
        					if(table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
        						table.getKeys().get(i).getColumns().add(columnName);
        						table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
        						columnName = null;
        						ordinalPosition = -1;
        						isAdded = true;
        						break;
        					}
        				}
        			}
        			if(isAdded) continue;
        			
        			if(pkey.getColumns() == null) pkey.setColumns(new ArrayList<String>());
        			if(columnName != null) {
        				pkey.getColumns().add(columnName);
        			}
        			if(pkey.getOrdinalPositions() == null) pkey.setOrdinalPositions(new ArrayList<Integer>());
        			if(ordinalPosition != -1) {
        				pkey.getOrdinalPositions().add(ordinalPosition);
        			}
        			pkey.setTableSchema(tableSchema);
        			pkey.setTableName(tableName);
        			pkey.setKeySchema(keySchema);
        			pkey.setName(keyName);
        			if (indexType == null){
        				pkey.setIndexType(IndexType.BTREE);
        			} else if(indexType.equals("HASH")) {
        				pkey.setIndexType(IndexType.HASH);
        			} else if (indexType.equals("BTREE")) {
        				pkey.setIndexType(IndexType.BTREE);
        			}
        			table.getKeys().add(pkey);
        		} else if (constraintType.equals("U")) {
        			String keySchema = (obj=result.get("constraint_schema")) != null ?obj.toString():null;
        			String keyName = (obj=result.get("constraint_name")) != null ?obj.toString():null;
        			String tableSchema = (obj=result.get("table_schema")) != null ?obj.toString():null;
        			String tableName = (obj=result.get("table_name")) != null ?obj.toString():null;
        			String columnName = (obj=result.get("column_name")) != null ?obj.toString():null;
        			obj = result.get("ordinal_position");
        			int ordinalPosition = -1;
        			if(obj!=null) ordinalPosition = Integer.valueOf(obj.toString());
        			String indexType = (obj=result.get("index_type")) != null ?obj.toString():null;
        			
        			UniqueKey ukey = new UniqueKey();
        			boolean isAdded = false;
        			for(int i=0; i<table.getKeys().size();i++) {
        				if(table.getKeys().get(i).getType().name().equals(Type.UNIQUE.name())) {
        					if(table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
        						table.getKeys().get(i).getColumns().add(columnName);
        						table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
        						columnName = null;
        						ordinalPosition = -1;
        						isAdded = true;
        						break;
        					}
        				}
        			}
        			if(isAdded) continue;
        			
        			if(ukey.getColumns() == null) ukey.setColumns(new ArrayList<String>());
        			if(columnName != null) {
        				ukey.getColumns().add(columnName);
        			}
        			if(ukey.getOrdinalPositions() == null) ukey.setOrdinalPositions(new ArrayList<Integer>());
        			if(ordinalPosition != -1) {
        				ukey.getOrdinalPositions().add(ordinalPosition);
        			}
        			ukey.setTableSchema(tableSchema);
        			ukey.setTableName(tableName);
        			ukey.setKeySchema(keySchema);
        			ukey.setName(keyName);
        			if (indexType == null){
        				ukey.setIndexType(IndexType.BTREE);
        			} else if(indexType.equals("HASH")) {
        				ukey.setIndexType(IndexType.HASH);
        			} else if (indexType.equals("BTREE")) {
        				ukey.setIndexType(IndexType.BTREE);
        			}
        			table.getKeys().add(ukey);
        		} else if (constraintType.equals("F")) {
        			String keySchema = (obj=result.get("constraint_schema")) != null ?obj.toString():null;
        			String keyName = (obj=result.get("constraint_name")) != null ?obj.toString():null;
        			String tableSchema = (obj=result.get("table_schema")) != null ?obj.toString():null;
        			String tableName = (obj=result.get("table_name")) != null ?obj.toString():null;
        			String columnName = (obj=result.get("column_name")) != null ?obj.toString():null;
        			obj = result.get("ordinal_position");
        			int ordinalPosition = -1;
        			if(obj!=null) ordinalPosition = Integer.valueOf(obj.toString());
        			String indexType = (obj=result.get("index_type")) != null ?obj.toString():null;
        			String refTableSchema = (obj=result.get("ref_table_schema")) != null ?obj.toString():null; 
        			String refTable = (obj=result.get("ref_table_name")) != null ?obj.toString():null;
        			String refColumnName = (obj=result.get("ref_column_name")) != null ?obj.toString():null;
        			String matchOption = (obj=result.get("match_option")) != null ?obj.toString():null; 
        			String updateRule = (obj=result.get("update_rule")) != null ?obj.toString():null;
        			String deleteRule = (obj=result.get("delete_rule")) != null ?obj.toString():null;
        			
        			
        			ForeignKey fkey = new ForeignKey();
        			
        			boolean isAdded = false;
        			
        			for(int i=0; i<table.getKeys().size();i++) {
        				if(table.getKeys().get(i).getType().name().equals(Type.FOREIGN.name())) {
        					ForeignKey key =  (ForeignKey)table.getKeys().get(i);
        					if(key.isSameKey(tableSchema, tableName, keySchema, keyName,refTableSchema,refTable)) {
        						if(fkey.getColumns() == null) fkey.setColumns(new ArrayList<String>());
        						key.getColumns().add(columnName);
        						if(fkey.getOrdinalPositions() == null) fkey.setOrdinalPositions(new ArrayList<Integer>());
        						key.getOrdinalPositions().add(ordinalPosition);
        						if(fkey.getRefColumns() == null) fkey.setRefColumns(new ArrayList<String>());
        						key.getRefColumns().add(refColumnName);
        						columnName = null;
        						ordinalPosition = -1;
        						isAdded = true;
        						break;
        					}
        				}
        			}
        			if(isAdded) continue;
        			
        			if(fkey.getColumns() == null) fkey.setColumns(new ArrayList<String>());
        			if(columnName != null) {
        				fkey.getColumns().add(columnName);
        			}
        			if(fkey.getRefColumns() == null) fkey.setRefColumns(new ArrayList<String>());
        			if(refColumnName != null) {
        				fkey.getRefColumns().add(refColumnName);
        			}
        			if(fkey.getOrdinalPositions() == null) fkey.setOrdinalPositions(new ArrayList<Integer>());
        			if(ordinalPosition != -1) {
        				fkey.getOrdinalPositions().add(ordinalPosition);
        			}
        			
        			if(fkey.getRefDef() == null) {
        				fkey.setRefDef(new ReferenceDefinition());
        			}
        			
        			if( matchOption != null ) {
        				if(matchOption.toUpperCase().contains("FULL")) {
        					fkey.getRefDef().setMatch(ForeignKeyMatch.FULL);
        				} else if(matchOption.toUpperCase().contains("PARTIAL")) {
        					fkey.getRefDef().setMatch(ForeignKeyMatch.PARTIAL);
        				} else if(matchOption.toUpperCase().contains("SIMPLE")) {
        					fkey.getRefDef().setMatch(ForeignKeyMatch.SIMPLE);
        				}
        			}
        			if( updateRule != null ) {
        				if(updateRule.toUpperCase().contains("RESTRICT")) {
        					fkey.getRefDef().setUpdate(ForeignKeyUpdate.RESTRICT);
        				} else if(updateRule.toUpperCase().contains("CASCADE")) {
        					fkey.getRefDef().setUpdate(ForeignKeyUpdate.CASCADE);
        				} else if(updateRule.toUpperCase().contains("SET") && updateRule.toUpperCase().contains("NULL")) {
        					fkey.getRefDef().setUpdate(ForeignKeyUpdate.SET_NULL);
        				} else if(updateRule.toUpperCase().contains("NO") && updateRule.toUpperCase().contains("ACTION")) {
        					fkey.getRefDef().setUpdate(ForeignKeyUpdate.NO_ACTION);
        				} else if(updateRule.toUpperCase().contains("SET") && updateRule.toUpperCase().contains("DEFAULT")) {
        					fkey.getRefDef().setUpdate(ForeignKeyUpdate.SET_DEFAULT);
        				}
        			}
        			if( deleteRule != null ) {
        				if(deleteRule.toUpperCase().contains("RESTRICT")) {
        					fkey.getRefDef().setDelete(ForeignKeyDelete.RESTRICT);
        				} else if(deleteRule.toUpperCase().contains("CASCADE")) {
        					fkey.getRefDef().setDelete(ForeignKeyDelete.CASCADE);
        				} else if(deleteRule.toUpperCase().contains("SET") && updateRule.toUpperCase().contains("NULL")) {
        					fkey.getRefDef().setDelete(ForeignKeyDelete.SET_NULL);
        				} else if(deleteRule.toUpperCase().contains("NO") && updateRule.toUpperCase().contains("ACTION")) {
        					fkey.getRefDef().setDelete(ForeignKeyDelete.NO_ACTION);
        				} else if(deleteRule.toUpperCase().contains("SET") && updateRule.toUpperCase().contains("DEFAULT")) {
        					fkey.getRefDef().setDelete(ForeignKeyDelete.SET_DEFAULT);
        				}
        			}
        			
        			fkey.setTableSchema(tableSchema);
        			fkey.setTableName(tableName);
        			fkey.setKeySchema(keySchema);
        			fkey.setName(keyName);
        			fkey.setRefTableSchema(refTableSchema);
        			fkey.setRefTable(refTable);
        			if (indexType == null){
        				fkey.setIndexType(IndexType.BTREE);
        			} else if(indexType.equals("HASH")) {
        				fkey.setIndexType(IndexType.HASH);
        			} else if (indexType.equals("BTREE")) {
        				fkey.setIndexType(IndexType.BTREE);
        			}
        			table.getKeys().add(fkey);
        		}
        		
        	}
        	Collections.sort(table.getColumns(),Column.getComparator());
			LogUtils.info(msgCode.getCode("C0040"),MySQLConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),MySQLConvertDBUtils.class);
		}
		return table;
	}
	
	
	/**
	 *<br>Index's schema name : index_schema (string)
	 *<br>Index's name : index_name (string)
	 *<br>Table's schema name : table_schema (string)
	 *<br>Table's name : table_name (string)
	 *<br>Column's name : column_name (string) 
	 *<br>Column's index ordinal position : ordinal_position (int)
	 *<br>Index's type : index_type (enum)
	  <br>&nbsp - HASH : hash index
	  <br>&nbsp - BTREE : btree index
	 * */
	public static Table setKeyInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info(msgCode.getCode("C0041"),MySQLConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYS)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
			}
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_KEY_INFORM, params));
			mew.run();
			List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
			LogUtils.info(msgCode.getCode("C0042") + results, ConvertDBUtils.class);
			Object obj = null;
        	for (Map<String,Object> result : results) {
        		obj = result.get("index_schema");
    			String keySchema = obj!=null?obj.toString():null;
    			obj = result.get("index_name");
    			String keyName = obj!=null?obj.toString():null;
    			obj = result.get("table_schema");
    			String tableSchema = obj!=null?obj.toString():null;
    			obj = result.get("table_name");
    			String tableName = obj!=null?obj.toString():null;
    			obj = result.get("column_name");
    			String columnName = obj!=null?obj.toString():null;
    			obj = result.get("ordinal_position");
    			int ordinalPosition = -1;
    			if(obj != null) ordinalPosition = Integer.valueOf(obj.toString());
    			obj = result.get("index_type");
    			String indexType = obj!=null?obj.toString():null;
    			
    			NormalKey nkey = new NormalKey();
    			
    			boolean isAdded = false;
    			for(int i=0; i<table.getKeys().size();i++) {
    				if(table.getKeys().get(i).getType().name().equals(Type.NORMAL.name())) {
    					if(table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
    						table.getKeys().get(i).getColumns().add(columnName);
    						table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
    						columnName = null;
    						ordinalPosition = -1;
    						isAdded = true;
    						break;
    					}
    				}
    			}
    			if(isAdded) continue;
    			if(nkey.getColumns() == null) nkey.setColumns(new ArrayList<String>());
    			if(columnName != null) {
    				nkey.getColumns().add(columnName);
    			}
    			if(nkey.getOrdinalPositions() == null) nkey.setOrdinalPositions(new ArrayList<Integer>());
    			if(ordinalPosition != -1) {
    				nkey.getOrdinalPositions().add(ordinalPosition);
    			}
    			nkey.setTableSchema(tableSchema);
    			nkey.setTableName(tableName);
    			nkey.setKeySchema(keySchema);
    			nkey.setName(keyName);
    			if (indexType == null){
    				nkey.setIndexType(IndexType.BTREE);
    			} else if(indexType.equals("HASH")) {
    				nkey.setIndexType(IndexType.HASH);
    			} else if (indexType.equals("BTREE")) {
    				nkey.setIndexType(IndexType.BTREE);
    			}
    			table.getKeys().add(nkey);
        	}
        	
        	Collections.sort(table.getColumns(),Column.getComparator());
			LogUtils.info(msgCode.getCode("C0043"),MySQLConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),MySQLConvertDBUtils.class);
		}
		return table;
	}

	
	public static List<View> setViewInform(String Schema, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<View> views = new ArrayList<View>();
		try {
			LogUtils.info(msgCode.getCode("C0044"),MySQLConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			
			params.put("TABLE_SCHEMA", Schema);
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_VIEW_INFORM, params));
			mew.run();
			List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
			LogUtils.info(msgCode.getCode("C0045")+results,MySQLConvertDBUtils.class);
			
			Object obj = null;
        	for (Map<String,Object> result : results) {
        		obj = result.get("TABLE_CATALOG");
    			String tableCatalLog = obj!=null?obj.toString():null;
    			obj = result.get("TABLE_SCHEMA");
    			String tableSchema = obj!=null?obj.toString():null;
    			obj = result.get("TABLE_NAME");
    			String tableName = obj!=null?obj.toString():null;
    			obj = result.get("VIEW_DEFINITION");
    			String viewDefinition = obj!=null?obj.toString():null;
    			obj = result.get("CHECK_OPTION");
    			String checkOption = obj!=null?obj.toString():null;
    			obj = result.get("IS_UPDATABLE");
    			String isUpdaTable = obj!=null?obj.toString():null;
    			
    			View view = new View();
    			boolean isAdded = false;
    			/*for(int i=0; i<table.getKeys().size();i++) {
    				if(table.getKeys().get(i).getType().name().equals(Type.VIEW.name())) {
    					if(table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
    						table.getKeys().get(i).getColumns().add(columnName);
    						table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
    						columnName = null;
    						ordinalPosition = -1;
    						isAdded = true;
    						break;
    					}
    				}
    			}*/
    			view.setTableCatalLog(tableCatalLog);
    			view.setTableSchema(tableSchema);
    			view.setTableName(tableName);
    			view.setViewDefinition(viewDefinition);
    			view.setCheckOption(checkOption);
    			view.setIsUpdaTable(isUpdaTable);
    			
    			views.add(view);
        	}       	
			LogUtils.info(msgCode.getCode("C0046"),MySQLConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),MySQLConvertDBUtils.class);
		}
		return views;
	}	
	
}
