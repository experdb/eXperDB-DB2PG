package com.k4m.experdb.db2pg.writer;

import java.sql.Connection;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;

public class DBWriter {
	//public static DBConfigInfo TAR_DB_CONFIG = new DBConfigInfo();
	
	private CopyIn copyIn = null;
	private String columnDelimiter = "\t";
	private Connection conn;
	private static int successByteCount;
	private static int insCnt = 0;
	
	boolean ContaintPool;
	private String poolName = "";
	
	public DBWriter(String poolName){
		this.poolName = poolName;
		DBCPPoolManagerConn();
	}
	
	public void DBCPPoolManagerConn() {
	}

	public void DBWrite(String lineStr, String table_nm) {
		try {

			conn = DBCPPoolManager.getConnection(poolName);
			
			CopyManager copyManager = new CopyManager(((DelegatingConnection<?>)conn).getInnermostDelegate().unwrap(BaseConnection.class));
			copyIn = copyManager.copyIn("COPY " + ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME + "." + table_nm + " FROM STDIN");
			//copyIn = copyManager.copyIn("COPY \"" + ConfigInfo.TAR_SCHEMA + "\".\"" + table_nm + "\" FROM STDIN WITH " + ConfigInfo.TAR_COPY_OPTIONS);
			//byte[] bytes = (lineStr + "\r\n").getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET);
			byte[] bytes = (lineStr).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET);
			copyIn.writeToCopy(bytes, 0, bytes.length);
			successByteCount +=bytes.length;
			insCnt += copyIn.endCopy();
		} catch (Exception e) {
			try {	
				if (copyIn != null) {
	    			copyIn.cancelCopy();	
				}
			}catch(Exception ee){
				ee.printStackTrace();
			}
			e.printStackTrace();
		}finally{
			try {					
				conn.commit();					
				conn.close();
			}catch(Exception e){
				e.printStackTrace();
			}
			System.out.println("[" + table_nm + "] successByteCnt : " + successByteCount);
			System.out.println("[" + table_nm + "] InsertCnt : " + insCnt);
		}
				
	}

}
