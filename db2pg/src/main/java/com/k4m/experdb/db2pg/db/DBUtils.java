package com.k4m.experdb.db2pg.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;

public class DBUtils {
	
	public static List<String> getTableNames(boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		Connection srcConn = null;
		PreparedStatement srcPreStmt = null;
		
		List<String> tableNames = new ArrayList<String>();
		
		QueryMaker qm = new QueryMaker("/src_mapper.xml");
		Map<String,Object> params = new HashMap<String,Object>();
		try {
			
			LogUtils.info("[START_GET_TABLE_NAMES]",DBUtils.class);
			srcConn = DBCPPoolManager.getConnection(srcPoolName);
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.ORA)){
				params.put("OWNER", dbConfigInfo.SCHEMA_NAME);
				params.put("TABLE_ONLY", tableOnly?"AND OBJECT_TYPE IN ('TABLE')":"AND OBJECT_TYPE IN ('TABLE','VIEW')");
				srcPreStmt = qm.getPreparedStatement("GET_TABLE_NAMES",Constant.DB_TYPE.ORA, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
				ResultSet rs = srcPreStmt.executeQuery();
	        	
	        	while (rs.next()){
	        		String tablename = rs.getString("OBJECT_NAME");
	        		tableNames.add(tablename);
	        	}
	        	rs.close();
			} else if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MSS)) {
				params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
				srcPreStmt = qm.getPreparedStatement("GET_TABLE_NAMES",Constant.DB_TYPE.MSS, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
	        	ResultSet rs = srcPreStmt.executeQuery();
	        	while (rs.next()) tableNames.add(rs.getString("TABLE_NAME"));
	        	rs.close();
			} else if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.ASE)) {
				params.put("USER_NAME", dbConfigInfo.SCHEMA_NAME);
				srcPreStmt = qm.getPreparedStatement("GET_TABLE_NAMES",Constant.DB_TYPE.ASE, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
	        	ResultSet rs = srcPreStmt.executeQuery();
	        	while (rs.next()) tableNames.add(rs.getString("table_name"));
	        	rs.close();
			} else if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
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
			LogUtils.error(e.getMessage(),DBUtils.class);
		} finally {
			LogUtils.info("[END_GET_TABLE_NAMES]",DBUtils.class);
		}
		return tableNames;
	}
	
//	public static String getCreateTableQuery(String table,String srcPoolName, DBConfigInfo dbConfigInfo) {
//		Connection srcConn = null;
//		PreparedStatement srcPreStmt = null;
//		String createTableQuery = null;
//		QueryMaker qm = new QueryMaker("/src_mapper.xml");
//		Map<String,Object> params = new HashMap<String,Object>();
//		try {
//			LogUtils.info("[START_CREATE_TABLE_QUERY_MAKE] "+table,DBUtils.class);
//			srcConn = DBCPPoolManager.getConnection(srcPoolName);
//			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
//				if(dbConfigInfo.SCHEMA_NAME!=null && !dbConfigInfo.SCHEMA_NAME.equals("")) {
//					params.put("SCHEMA", dbConfigInfo.SCHEMA_NAME+".");
//				} else {
//					params.put("SCHEMA", "");
//				}
//				if(table!=null && !table.equals("")) {
//					params.put("TABLE", table);
//				} else {
//					throw new Exception("TABLE NOT FOUND");
//				}
//				srcPreStmt = qm.getPreparedStatement("GET_CREATE_TABLE",Constant.DB_TYPE.MYSQL, params, srcConn, Double.parseDouble(dbConfigInfo.DB_VER));
//				ResultSet rs = srcPreStmt.executeQuery();
//				if(rs.first()) createTableQuery = rs.getString("Create Table");
//				rs.close();
//			}
//			srcConn.close();
//		} catch(Exception e){
//			LogUtils.error(e.getMessage(),DBUtils.class);
//		} finally {
//			LogUtils.info("[END_CREATE_TABLE_QUERY_MAKE] "+table,DBUtils.class);
//		}
//		return createTableQuery;
//	}
	
}
