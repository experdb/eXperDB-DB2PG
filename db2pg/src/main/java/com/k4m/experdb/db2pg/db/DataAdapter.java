package com.k4m.experdb.db2pg.db;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.CommonUtil;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.datastructure.DataTable;

public class DataAdapter {
	private Connection conn;
	private PreparedStatement pStmt = null;
	private String PoolName;
	private boolean IsTransaction;
	private DataAdapter(String PoolName){
		this.PoolName = PoolName;
		this.IsTransaction = false;
	}
	public static DataAdapter getInstance(String PoolName) {
		return new DataAdapter(PoolName);
	}
	
	public void BeginTransaction() throws Exception{
		conn = DBCPPoolManager.getConnection(PoolName);
		
		this.IsTransaction = true;
	}
	
	public void RollBackTransaction() throws Exception{
		conn.rollback();
		this.IsTransaction = false;		
		CloseConn(null);
	}
	
	public void EndTransaction() throws Exception{
		conn.commit();
		this.IsTransaction = false;		
		CloseConn(null);
	}
	
	//실행 중인 명령어 취소
	public void Cancel() throws Exception{
		if (pStmt != null) {			
			pStmt.cancel();
		}
	}
	
	/************************************************************
	 * Select
	 ************************************************************/
	public  DataTable Fill(String sql, List<Object> binds) throws Exception{
		//PreparedStatement pStmt = null;
		ResultSet rs = null;
		DataTable dt = new DataTable();;
		//Connection conn = null;
		
		try{
			
			if (conn == null) {
				conn = DBCPPoolManager.getConnection(PoolName);				
			}
			
			pStmt = conn.prepareStatement(sql);
			
			//OraclePreparedStatement stmt = ((OraclePreparedStatement)((DelegatingPreparedStatement)pStmt).getInnermostDelegate());
			pStmt.setFetchSize(2000);
			//stmt.setLobPrefetchSize(25000);
			
			if (binds != null) {
				for (int i = 1; i<= binds.size(); i++) {
					PreparedStmtSetValue(pStmt, i, binds.get(i-1));
				}	
			}
			
			rs = pStmt.executeQuery();
						
			ResultSetMetaData rsmd = rs.getMetaData();
	
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				dt.AddColumns(rsmd.getColumnLabel(i).toUpperCase());
			}

			while (rs.next()) {
				List<Object> list = new ArrayList<Object>();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {		
					list.add(DatabaseUtil.PreparedStmtSetValue(rsmd.getColumnType(i), rs, i));
				}
				dt.AddRow(list);
			}
			
			return dt;
		} catch(SQLException e){
			LogUtils.error(CommonUtil.getStackTrace(e.getNextException()),DataAdapter.class);
			//return dt;
			throw e;
		} catch(Exception ee){
			LogUtils.error(CommonUtil.getStackTrace(ee),DataAdapter.class);
			throw ee;
		}finally {		
			CloseConn(pStmt);
		}
	}
	
	/************************************************************
	 * Insert
	 ************************************************************/
	public  int Insert(String sql, DataTable dt) throws Exception{
		//PreparedStatement pStmt = null;
		//Connection conn = null;
		try{
			if (conn == null) {
				conn = DBCPPoolManager.getConnection(PoolName);
				
			}

			
			pStmt = conn.prepareStatement(sql);
			
			int i = 1;
			for (Map<String, Object> binds : dt.getRows()) {				
				for (String columnName : dt.getColumns()) {					
					PreparedStmtSetValue(pStmt, i, binds.get(columnName));
					i++;
				}
				pStmt.addBatch();
				i = 1;
			}
			
			int ret[] = pStmt.executeBatch();
			int tot = 0;
			if (ret != null) {
				for (int val : ret) {
					if (val > -3) {
						tot += val;
					}
					else if (val <= -3) {
							
					}					
				}
			}
			return tot;
		} catch(SQLException e){
			LogUtils.error(CommonUtil.getStackTrace(e.getNextException()),DataAdapter.class);
			throw e;
		}finally {
			if (conn != null && IsTransaction == false) {
				conn.commit();
			}
			
			CloseConn(pStmt);
		}
	}
	
	/************************************************************
	 * Update or Delete
	 ************************************************************/
	public  int DeleteOrUpdate(String sql, List<Object> binds) throws Exception{
		//PreparedStatement pStmt = null;
		//Connection conn = null;
		
		try{
			if (conn == null) {
				conn = DBCPPoolManager.getConnection(PoolName);
				
			}

			pStmt = conn.prepareStatement(sql);
			
			if (binds != null) {
				for (int i = 1; i<= binds.size(); i++) {
					PreparedStmtSetValue(pStmt, i, binds.get(i-1));
				}	
			}
			
			int ret = pStmt.executeUpdate();

			return ret;
		}catch(SQLException e){
			LogUtils.error(CommonUtil.getStackTrace(e.getNextException()),DataAdapter.class);
			throw e;
		} finally {
			if (conn != null && IsTransaction == false) {
				conn.commit();
			}
			
			CloseConn(pStmt);
		}
	}	
	
	/************************************************************
	 * DDL
	 ************************************************************/
	public int ExecuteDDL(String sql) throws Exception{
		//PreparedStatement pStmt = null;

		try{
			if (conn == null) {
				conn = DBCPPoolManager.getConnection(PoolName);
				
			}

			pStmt = conn.prepareStatement(sql);
			
			pStmt.execute();
			
			if (pStmt.getWarnings() != null){
				return -(pStmt.getWarnings().getErrorCode());
			}
			
			return 0;			
		}catch(SQLException e){
			LogUtils.error(CommonUtil.getStackTrace(e.getNextException()),DataAdapter.class);
			throw e;
		}
		finally {			
			CloseConn(pStmt);
		}
	}
	/************************************************************
	 * Call Procedure(One only Out Parameter)
	 ************************************************************/
	public int CallProcedure(String sql, List<CallableParameter> binds) throws Exception{		
		CallableStatement cStmt = null;
		try{
			if (conn == null) {
				conn = DBCPPoolManager.getConnection(PoolName);
				
			}

			cStmt = conn.prepareCall(sql);
			int retIdx = 0;
			if (binds != null) {
				for (int i = 1; i<= binds.size(); i++) {
					if (binds.get(i - 1).getType() == CallableType.IN){
						cStmt.setObject(i, binds.get(i - 1).getValue());
					}else{
						retIdx = i;
						cStmt.registerOutParameter(i, binds.get(i - 1).getOutValueType());
					}
					
				}	
			}
			
			int ret = cStmt.executeUpdate();
			
			if (conn != null && IsTransaction == false) {
				conn.commit();
			}
			
			if (retIdx != 0){
				binds.get(retIdx).setValue(cStmt.getObject(retIdx));
			}
			
			return ret;
		}catch(SQLException e){
			LogUtils.error(CommonUtil.getStackTrace(e.getNextException()),DataAdapter.class);
			throw e;
		}finally {
			CloseConn(cStmt);
		}
	}	
	
	
	private  void PreparedStmtSetValue(PreparedStatement pStmt, int idx, Object obj) throws SQLException{
		if (obj instanceof String) {
			pStmt.setString(idx, (String) obj);
		} else if(obj instanceof Integer){
			pStmt.setInt(idx, (Integer) obj);
		} else if(obj instanceof BigDecimal){
				pStmt.setBigDecimal(idx, (BigDecimal) obj);
		} else if(obj instanceof Double){
			pStmt.setDouble(idx, (Double) obj);
		} else if(obj instanceof Date){
			pStmt.setDate(idx, (Date) obj);
		} else if(obj instanceof byte[]){
			pStmt.setBytes(idx, (byte[]) obj);			
		} else{
			pStmt.setObject(idx, obj);
		}
	}
	
	private  void PreparedStmtSetValue(CallableStatement cStmt, int idx, Object obj) throws SQLException{
		if (obj instanceof String) {
			pStmt.setString(idx, (String) obj);
		} else if(obj instanceof Integer){
			pStmt.setInt(idx, (Integer) obj);
		} else if(obj instanceof BigDecimal){
				pStmt.setBigDecimal(idx, (BigDecimal) obj);
		} else if(obj instanceof Double){
			pStmt.setDouble(idx, (Double) obj);
		} else if(obj instanceof Date){
			pStmt.setDate(idx, (Date) obj);
		} else if(obj instanceof byte[]){
			pStmt.setBytes(idx, (byte[]) obj);			
		} else{
			pStmt.setObject(idx, obj);
		}
	}
	
	public void CloseConn(PreparedStatement pStmt) {
		try{
			if(pStmt != null) {
				pStmt.close();
			}
			if (conn != null && IsTransaction == false) {
				conn.commit();
				conn.close();
				conn = null;
			}	
		}catch(Exception e){
			LogUtils.error(CommonUtil.getStackTrace(e),DataAdapter.class);
		}
	}
	
	public void CloseConn(CallableStatement cStmt) {
		try{
			if(cStmt != null) {
				cStmt.close();
			}
			if (conn != null && IsTransaction == false) {
				conn.commit();
				conn.close();
				conn = null;
			}	
		}catch(Exception e){
			LogUtils.error(CommonUtil.getStackTrace(e),DataAdapter.class);
		}
	}
}
