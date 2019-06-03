package com.k4m.experdb.db2pg.unload;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.rebuild.TargetPgDDL;

public class ManagementConstraint {
	
	
	//FK drop
	public void dropFk(TargetPgDDL dbInform) throws Exception {
		
		List<String> fkDropList = dbInform.getFkDropList();
		
		if(fkDropList.size() > 0) {
			for(String fkDropSql : fkDropList) {
				execSql(Constant.POOLNAME.TARGET.name(), fkDropSql);
			}
		}
	}
	
	//Index drop
	public void dropIndex(TargetPgDDL dbInform) throws Exception {
		List<String> indexDropList = dbInform.getIdxDropList();
		
		if(indexDropList.size() > 0) {
			for(String indexDropSql : indexDropList) {
				execSql(Constant.POOLNAME.TARGET.name(), indexDropSql);
			}
		}
	}
	
	//createFK
	public void createFk(TargetPgDDL dbInform) throws Exception {
		
		List<String> fkCreateList = dbInform.getFkCreateList();
		
		if(fkCreateList.size() > 0) {
			for(String fkCreateSql : fkCreateList) {
				execSql(Constant.POOLNAME.TARGET.name(), fkCreateSql);
			}
		}
	}
	
	//createIndex
	public void createIndex(TargetPgDDL dbInform) throws Exception {
		
		List<String> idxCreateList = dbInform.getIdxCreateList();
		
		if(idxCreateList.size() > 0) {
			for(String idxCreateSql : idxCreateList) {
				execSql(Constant.POOLNAME.TARGET.name(), idxCreateSql);
			}
		}
	}
	
	private void execSql(String poolName, String sql) throws Exception {
		PreparedStatement psmt = null;
		
		Connection conn = DBCPPoolManager.getConnection(poolName);
		
		try {
			psmt = conn.prepareStatement(sql);
			psmt.execute();
	    	conn.commit();
		} catch(Exception e) {
			conn.rollback();
			throw e;
		} finally {
			CloseConn(conn, psmt);
		}
	}
	
	private void CloseConn(Connection conn, Statement stmt) {
		try{
			if(stmt != null) {
				stmt.close();
			}

			if (conn != null && !conn.isClosed()) {
				conn.close();
				conn = null;
			}	
		}catch(Exception e){
			LogUtils.error(e.getMessage(),ExecuteQuery.class,e);
		}
	}
}
