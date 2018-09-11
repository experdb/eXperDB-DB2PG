package com.k4m.experdb.db2pg.writer;

import java.sql.Connection;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;

public class DBWriter {
	public static DBConfigInfo TAR_DB_CONFIG = new DBConfigInfo();
	
	private CopyIn copyIn = null;
	private String columnDelimiter = "\t";
	private Connection conn;
	private static int successByteCount;
	private static int insCnt = 0;
	
	boolean ContaintPool;
	
	public DBWriter(){
		DBCPPoolManagerConn();
	}
	
	public void DBCPPoolManagerConn() {
		TAR_DB_CONFIG.SERVERIP = "222.110.153.137";
		TAR_DB_CONFIG.DBNAME = "migratordb";
		TAR_DB_CONFIG.SCHEMA_NAME = "migrator";
		TAR_DB_CONFIG.USERID = "migrator";
		TAR_DB_CONFIG.PORT = "5432";
		TAR_DB_CONFIG.DB_PW = "webcash123!@#";  		
		TAR_DB_CONFIG.CHARSET ="UTF-8";
		TAR_DB_CONFIG.DB_TYPE ="POG";

    	try {	
    		// 풀이 존재하는지 확인 (존재: true 미존재:false)
    		ContaintPool = DBCPPoolManager.ContaintPool("TEST");
    		if(ContaintPool == false){
    			DBCPPoolManager.setupDriver(TAR_DB_CONFIG, "TEST", 1);
    		}
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
	}

	public void DBWrite(String lineStr, String table_nm) {
		try {
			System.out.println(lineStr);
			conn =DBCPPoolManager.getConnection("TEST");
			
			CopyManager copyManager = new CopyManager(((DelegatingConnection<?>)conn).getInnermostDelegate().unwrap(BaseConnection.class));
			copyIn = copyManager.copyIn("COPY \"" + TAR_DB_CONFIG.SCHEMA_NAME + "\".\"" + table_nm + "\" FROM STDIN");
			//copyIn = copyManager.copyIn("COPY \"" + ConfigInfo.TAR_SCHEMA + "\".\"" + table_nm + "\" FROM STDIN WITH " + ConfigInfo.TAR_COPY_OPTIONS);
			byte[] bytes = (lineStr + "\r\n").getBytes(TAR_DB_CONFIG.CHARSET);
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
