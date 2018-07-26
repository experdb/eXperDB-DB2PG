package com.k4m.experdb.db2pg.convert.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
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
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker.WORK_TYPE;

public class ConvertDBUtils {
	
	@SuppressWarnings("unchecked")
	public static List<String> getTableNames(boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info("[START_GET_TABLE_NAMES]",ConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
//				params.put("TABLE_ONLY", tableOnly?"WHERE table_type IN ('BASE TABLE')":"WHERE table_type IN ('BASE TABLE','VIEW')");
				params.put("TABLE_ONLY", "AND table_type IN ('BASE TABLE')");
			}
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_TABLE_NAMES, params));
			mew.run();
			
			return (List<String>) mew.getListResult();
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		} finally {
			LogUtils.info("[END_GET_TABLE_NAMES]",ConvertDBUtils.class);
		}
		return null;
	}
	
	public static List<Table> getTableInform(List<String> tableNames,boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<Table> tables = new ArrayList<Table>();
		try {
			LogUtils.info("[START_GET_TABLE_INFORM]",ConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
				params.put("TABLE_ONLY", "AND table_type IN ('BASE TABLE')");
				if(tableNames != null && !tableNames.isEmpty()) {
					StringBuilder sb = new StringBuilder();
					sb.append("AND table_name IN (");
					for(String tableName : tableNames) {
						sb.append("'");
						sb.append(tableName);
						sb.append("',");
					}
					sb.deleteCharAt(sb.length()-1);
					sb.append(")");
					params.put("TABLE_LIST", sb.toString());
				} else {
					params.put("TABLE_LIST", "");
				}
				MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_TABLE_INFORM, params));
				mew.run();
				@SuppressWarnings("unchecked")
				List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
				for (Map<String,Object> result : results) {
					Table table = new Table();
					table.setSchemaName((String)result.get("table_schema"));
					table.setName((String)result.get("table_name"));
					table.setAutoIncrement((Integer)result.get("auto_increment"));
					table.setComment((String)result.get("table_comment"));
					tables.add(table);
				}
			}
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		} finally {
			LogUtils.info("[END_GET_TABLE_INFORM]",ConvertDBUtils.class);
		}
		
		return tables;
	}
	
	public static Table setColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			
			LogUtils.info("[START_SET_COLUMN_INFORM]",ConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
				MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_COLUMN_INFORM, params));
				mew.run();
				@SuppressWarnings("unchecked")
				List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
	        	
	        	for (Map<String,Object> result : results) {
	        		Column column = new Column();
	        		column.setOrdinalPosition((Integer)result.get("ordinal_position"));
	        		column.setName((String)result.get("column_name"));
	        		column.setDefaultValue((String)result.get("column_default"));
	        		column.setNotNull(!(Boolean)result.get("is_nullable"));
	        		column.setNumericPrecision((Integer)result.get("numeric_precision"));
	        		column.setNumericScale((Integer)result.get("numeric_scale"));
	        		String columnType = (String)result.get("column_type");
	        		column.setType(columnType);
	        		column.setComment((String)result.get("column_comment"));
	        		column.setAutoIncreament(((String)result.get("extra")).contains("auto_increment"));
	        		column.setExtra((String)result.get("extra"));
	        		table.getColumns().add(column);
	        	}
	        	Collections.sort(table.getColumns(),Column.getComparator());
			}
			LogUtils.info("[END_SET_COLUMN_INFORM]",ConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		}
		return table;
	}
	
	public static Table setConstraintInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info("[START_SET_CONSTRAINT_INFORM]",ConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
				MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_CONSTRAINT_INFORM, params));
				mew.run();
				@SuppressWarnings("unchecked")
				List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
				
	        	for(Map<String,Object> result : results ) {
	        		String constraintType = (String)result.get("constraint_type");
	        		if(constraintType.equals("PRIMARY KEY")) {
	        			String keySchema = (String)result.get("constraint_schema");
	        			String keyName = (String)result.get("constraint_name");
	        			String tableSchema = (String)result.get("table_schema");
	        			String tableName = (String)result.get("table_name");
	        			String columnName = (String)result.get("column_name");
	        			int ordinalPosition = (Integer)result.get("ordinal_position");
	        			String indexType = (String)result.get("index_type");
	        			
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
	        		} else if (constraintType.equals("UNIQUE")) {
	        			String keySchema = (String)result.get("constraint_schema");
	        			String keyName = (String)result.get("constraint_name");
	        			String tableSchema = (String)result.get("table_schema");
	        			String tableName = (String)result.get("table_name");
	        			String columnName = (String)result.get("column_name");
	        			int ordinalPosition = (Integer)result.get("ordinal_position");
	        			String indexType = (String)result.get("index_type");
	        			
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
	        		} else if (constraintType.equals("FOREIGN KEY")) {
	        			String keySchema = (String)result.get("constraint_schema");
	        			String keyName = (String)result.get("constraint_name");
	        			String tableSchema = (String)result.get("table_schema");
	        			String tableName = (String)result.get("table_name");
	        			String columnName = (String)result.get("column_name");
	        			int ordinalPosition = (Integer)result.get("ordinal_position");
	        			String indexType = (String)result.get("index_type");
	        			String refTableSchema = (String)result.get("referenced_table_schema");
	        			String refTable = (String)result.get("referenced_table_name");
	        			String refColumnName = (String)result.get("referenced_column_name");
	        			String matchOption = (String)result.get("match_option"); 
	        			String updateRule = (String)result.get("update_rule");
	        			String deleteRule = (String)result.get("delete_rule");
	        			
	        			
//	        			private ReferenceDefinition refDef;
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
			}
			LogUtils.info("[END_SET_CONSTRAINT_INFORM]",ConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		}
		return table;
	}
	
	
	public static Table setKeyInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info("[START_SET_KEY_INFORM]",ConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
				MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_KEY_INFORM, params));
				mew.run();
	        	@SuppressWarnings("unchecked")
				List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
	        	for (Map<String,Object> result : results) {
        			String keySchema = (String)result.get("index_schema");
        			String keyName = (String)result.get("index_name");
        			String tableSchema = (String)result.get("table_schema");
        			String tableName = (String)result.get("table_name");
        			String columnName = (String)result.get("column_name");
        			int ordinalPosition = (Integer)result.get("seq_in_index");
        			String indexType = (String)result.get("index_type");
        			
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
			}
			LogUtils.info("[END_SET_KEY_INFORM]",ConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		}
		return table;
	}
	
	public static String getCreateTableQuery(String table,String srcPoolName, DBConfigInfo dbConfigInfo) {
		String createTableQuery = null;
		try {
			LogUtils.info("[START_CREATE_TABLE_QUERY_MAKE] "+table,ConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				if(dbConfigInfo.SCHEMA_NAME!=null && !dbConfigInfo.SCHEMA_NAME.equals("")) {
					params.put("SCHEMA", dbConfigInfo.SCHEMA_NAME+".");
				} else {
					params.put("SCHEMA", "");
				}
				if(table!=null && !table.equals("")) {
					params.put("TABLE", table);
				} else {
					throw new Exception("TABLE NOT FOUND");
				}
				MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_KEY_INFORM, params));
				mew.run();
				@SuppressWarnings("unchecked")
				List<String> results = (List<String>)mew.getListResult();
				
				if(!results.isEmpty()) createTableQuery = results.get(0);
			}
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		} finally {
			LogUtils.info("[END_CREATE_TABLE_QUERY_MAKE] "+table,ConvertDBUtils.class);
		}
		return createTableQuery;
	}
	
}
