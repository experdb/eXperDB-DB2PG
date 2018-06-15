package com.k4m.experdb.db2pg.convert.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.table.key.ForeignKey;
import com.k4m.experdb.db2pg.convert.table.key.Key;
import com.k4m.experdb.db2pg.convert.table.key.Key.IndexType;
import com.k4m.experdb.db2pg.convert.table.key.Key.Type;
import com.k4m.experdb.db2pg.convert.table.key.NormalKey;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyDelete;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyMatch;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyUpdate;
import com.k4m.experdb.db2pg.convert.table.key.option.ReferenceDefinition;
import com.k4m.experdb.db2pg.convert.table.key.PrimaryKey;
import com.k4m.experdb.db2pg.convert.table.key.UniqueKey;
import com.k4m.experdb.db2pg.convert.vo.ConvertVO;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.QueryMaker;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;

public class ConvertDBUtils {
	
	public static List<String> getTableNames(boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		Connection srcConn = null;
		PreparedStatement srcPreStmt = null;
		
		List<String> tableNames = new ArrayList<String>();
		
		QueryMaker qm = new QueryMaker("/src_mapper.xml");
		Map<String,Object> params = new HashMap<String,Object>();
		try {
			
			LogUtils.info("[START_GET_TABLE_NAMES]",ConvertDBUtils.class);
			srcConn = DBCPPoolManager.getConnection(srcPoolName);
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
//				params.put("TABLE_ONLY", tableOnly?"WHERE table_type IN ('BASE TABLE')":"WHERE table_type IN ('BASE TABLE','VIEW')");
				params.put("TABLE_ONLY", "AND table_type IN ('BASE TABLE')");
				srcPreStmt = qm.getPreparedStatement("GET_TABLE_NAMES",Constant.DB_TYPE.MYSQL, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
	        	ResultSet rs = srcPreStmt.executeQuery();
	        	while (rs.next()) tableNames.add(rs.getString("table_name"));
	        	rs.close();
			}
			srcPreStmt.close();
			srcConn.close();
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		} finally {
			LogUtils.info("[END_GET_TABLE_NAMES]",ConvertDBUtils.class);
		}
		return tableNames;
	}
	
	public static List<Table> getTableInform(List<String> tableNames,boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		Connection srcConn = null;
		PreparedStatement srcPreStmt = null;
		
		List<Table> tables = new ArrayList<Table>();
		
		QueryMaker qm = new QueryMaker("/src_mapper.xml");
		Map<String,Object> params = new HashMap<String,Object>();
		try {
			LogUtils.info("[START_GET_TABLE_INFORM]",ConvertDBUtils.class);
			srcConn = DBCPPoolManager.getConnection(srcPoolName);
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
				srcPreStmt = qm.getPreparedStatement("GET_TABLE_INFORM",Constant.DB_TYPE.MYSQL, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
				ResultSet rs = srcPreStmt.executeQuery();
				while (rs.next()) {
					Table table = new Table();
					table.setSchemaName(rs.getString("table_schema"));
					table.setName(rs.getString("table_name"));
					table.setAutoIncrement(rs.getLong("auto_increment"));
					table.setComment(rs.getString("table_comment"));
					
					tables.add(table);
				}
				rs.close();
			}
			srcPreStmt.close();
			srcConn.close();
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		} finally {
			LogUtils.info("[END_GET_TABLE_INFORM]",ConvertDBUtils.class);
		}
		
		return tables;
	}
	
	public static Table setColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		Connection srcConn = null;
		PreparedStatement srcPreStmt = null;
		
		QueryMaker qm = new QueryMaker("/src_mapper.xml");
		Map<String,Object> params = new HashMap<String,Object>();
		try {
			
			LogUtils.info("[START_SET_COLUMN_INFORM]",ConvertDBUtils.class);
			srcConn = DBCPPoolManager.getConnection(srcPoolName);
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
				srcPreStmt = qm.getPreparedStatement("GET_COLUMN_INFORM",Constant.DB_TYPE.MYSQL, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));

	        	ResultSet rs = srcPreStmt.executeQuery();
	        	
	        	while (rs.next()) {
	        		Column column = new Column();
	        		column.setOrdinalPosition(rs.getInt("ordinal_position"));
	        		column.setName(rs.getString("column_name"));
	        		column.setDefaultValue(rs.getString("column_default"));
	        		column.setNotNull(!rs.getBoolean("is_nullable"));
	        		column.setNumericPrecision(rs.getInt("numeric_precision"));
	        		column.setNumericScale(rs.getInt("numeric_scale"));
	        		String columnType = rs.getString("column_type");
	        		column.setType(columnType);
	        		column.setComment(rs.getString("column_comment"));
	        		column.setAutoIncreament(rs.getString("extra").contains("auto_increment"));
	        		column.setExtra(rs.getString("extra"));
	        		table.getColumns().add(column);
	        	}
	        	Collections.sort(table.getColumns(),Column.getComparator());
	        	rs.close();
			}
			srcPreStmt.close();
			srcConn.close();
			LogUtils.info("[END_SET_COLUMN_INFORM]",ConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		}
		return table;
	}
	
	public static Table setConstraintInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		Connection srcConn = null;
		PreparedStatement srcPreStmt = null;
		
		QueryMaker qm = new QueryMaker("/src_mapper.xml");
		Map<String,Object> params = new HashMap<String,Object>();
		try {
			
			LogUtils.info("[START_SET_CONSTRAINT_INFORM]",ConvertDBUtils.class);
			srcConn = DBCPPoolManager.getConnection(srcPoolName);
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
				srcPreStmt = qm.getPreparedStatement("GET_CONSTRAINT_INFORM",Constant.DB_TYPE.MYSQL, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
				
	        	ResultSet rs = srcPreStmt.executeQuery();
	        	while (rs.next()) {
	        		String constraintType = rs.getString("constraint_type");
	        		if(constraintType.equals("PRIMARY KEY")) {
	        			String keySchema = rs.getString("constraint_schema");
	        			String keyName = rs.getString("constraint_name");
	        			String tableSchema = rs.getString("table_schema");
	        			String tableName = rs.getString("table_name");
	        			String columnName = rs.getString("column_name");
	        			int ordinalPosition = rs.getInt("ordinal_position");
	        			String indexType = rs.getString("index_type");
	        			
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
	        			String keySchema = rs.getString("constraint_schema");
	        			String keyName = rs.getString("constraint_name");
	        			String tableSchema = rs.getString("table_schema");
	        			String tableName = rs.getString("table_name");
	        			String columnName = rs.getString("column_name");
	        			int ordinalPosition = rs.getInt("ordinal_position");
	        			String indexType = rs.getString("index_type");
	        			
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
	        			String keySchema = rs.getString("constraint_schema");
	        			String keyName = rs.getString("constraint_name");
	        			String tableSchema = rs.getString("table_schema");
	        			String tableName = rs.getString("table_name");
	        			String columnName = rs.getString("column_name");
	        			int ordinalPosition = rs.getInt("ordinal_position");
	        			String indexType = rs.getString("index_type");
	        			String refTableSchema = rs.getString("referenced_table_schema");
	        			String refTable = rs.getString("referenced_table_name");
	        			String refColumnName = rs.getString("referenced_column_name");
	        			String matchOption = rs.getString("match_option"); 
	        			String updateRule = rs.getString("update_rule");
	        			String deleteRule = rs.getString("delete_rule");
	        			
	        			
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
	        	rs.close();
			}
			srcPreStmt.close();
			srcConn.close();
			LogUtils.info("[END_SET_CONSTRAINT_INFORM]",ConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		}
		return table;
	}
	
	
	public static Table setKeyInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		Connection srcConn = null;
		PreparedStatement srcPreStmt = null;
		
		QueryMaker qm = new QueryMaker("/src_mapper.xml");
		Map<String,Object> params = new HashMap<String,Object>();
		try {
			
			LogUtils.info("[START_SET_KEY_INFORM]",ConvertDBUtils.class);
			srcConn = DBCPPoolManager.getConnection(srcPoolName);
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", table.getSchemaName());
				params.put("TABLE_NAME", table.getName());
				srcPreStmt = qm.getPreparedStatement("GET_KEY_INFORM",Constant.DB_TYPE.MYSQL, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
	        	ResultSet rs = srcPreStmt.executeQuery();
	        	
	        	while (rs.next()) {
        			String keySchema = rs.getString("index_schema");
        			String keyName = rs.getString("index_name");
        			String tableSchema = rs.getString("table_schema");
        			String tableName = rs.getString("table_name");
        			String columnName = rs.getString("column_name");
        			int ordinalPosition = rs.getInt("seq_in_index");
        			String indexType = rs.getString("index_type");
        			
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
	        	rs.close();
			}
			srcPreStmt.close();
			srcConn.close();
			LogUtils.info("[END_SET_KEY_INFORM]",ConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		}
		return table;
	}
	
	public static String getCreateTableQuery(String table,String srcPoolName, DBConfigInfo dbConfigInfo) {
		Connection srcConn = null;
		PreparedStatement srcPreStmt = null;
		String createTableQuery = null;
		QueryMaker qm = new QueryMaker("/src_mapper.xml");
		Map<String,Object> params = new HashMap<String,Object>();
		try {
			LogUtils.info("[START_CREATE_TABLE_QUERY_MAKE] "+table,ConvertDBUtils.class);
			srcConn = DBCPPoolManager.getConnection(srcPoolName);
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
				srcPreStmt = qm.getPreparedStatement("GET_CREATE_TABLE",Constant.DB_TYPE.MYSQL, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
				ResultSet rs = srcPreStmt.executeQuery();
				if(rs.first()) createTableQuery = rs.getString("Create Table");
				rs.close();
			}
			srcConn.close();
		} catch(Exception e){
			LogUtils.error(e.getMessage(),ConvertDBUtils.class);
		} finally {
			LogUtils.info("[END_CREATE_TABLE_QUERY_MAKE] "+table,ConvertDBUtils.class);
		}
		return createTableQuery;
	}
	
}
