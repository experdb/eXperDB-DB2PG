package com.k4m.experdb.db2pg.writer;

import java.sql.Connection;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.k4m.experdb.db2pg.common.StrUtil;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;

public class DBWriter {
	//public static DBConfigInfo TAR_DB_CONFIG = new DBConfigInfo();
	
	private CopyIn copyIn = null;
	private Connection conn;
	private static int successByteCount;
	private static int insCnt = 0;
	private int errLine = -1;
	
	boolean ContaintPool;
	private String poolName = "";
	
	
	public int getErrLine() {
		return errLine;
	}

	public void setErrLine(int errLine) {
		this.errLine = errLine;
	}

	public DBWriter(String poolName){
		this.poolName = poolName;
	}
	
	public void DBCPPoolManagerConn() {
	}

	public void DBWrite(String lineStr, String table_nm) throws Exception {
		try {
			insCnt = 0;
			
			conn = DBCPPoolManager.getConnection(poolName);
			
			CopyManager copyManager = new CopyManager(((DelegatingConnection<?>)conn).getInnermostDelegate().unwrap(BaseConnection.class));
			copyIn = copyManager.copyIn("COPY " + ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME + "." + table_nm + " FROM STDIN");

			byte[] bytes = (lineStr).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET);
			copyIn.writeToCopy(bytes, 0, bytes.length);
			successByteCount +=bytes.length;
			insCnt += copyIn.endCopy();
			
			conn.commit();
			
		} catch (Exception e) {
			try {	
				String strErrLine = StrUtil.strGetLine(e.toString());
				int intErrLine = -1;
				if(strErrLine != null && strErrLine.equals("")) {
					intErrLine = Integer.parseInt(strErrLine);
				}
				this.setErrLine(intErrLine);
				
				conn.rollback();
				if (copyIn != null) copyIn.cancelCopy();	
	
			}catch(Exception ee){
			}
			
			throw e;

		}finally{
			try {										
				conn.close();
			}catch(Exception e){
			}
			System.out.println("[" + table_nm + "] successByteCnt : " + successByteCount);
			System.out.println("[" + table_nm + "] InsertCnt : " + insCnt);
		}
				
	}
	
	public static void main(String[] args) throws Exception {
		
		String str = "ERROR:  duplicate key value violates unique constraint \"test_pkey\" DETAIL:  Key (id)=(5) already exists. CONTEXT:  COPY test, line 61";
		
		int idx = str.indexOf("line");
		
		String strLine = str.substring(idx+5);
		
		System.out.println(" line:" + strLine);
	}

}
