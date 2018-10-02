package com.k4m.experdb.db2pg.writer;

import java.sql.Connection;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.common.StrUtil;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;

public class DBWriter {
	//public static DBConfigInfo TAR_DB_CONFIG = new DBConfigInfo();
	
	private CopyIn copyIn = null;
	private Connection conn;
	private static int processBytes = 0;
	private static int processLines = 0;
	private int processErrorLInes = 0;
	private int errLine = -1;
	private int errCount = 0;
	
	boolean ContaintPool;
	private String poolName = "";
	
	
	public int getProcessErrorLInes() {
		return processErrorLInes;
	}

	public void setProcessErrorLInes(int processErrorLInes) {
		this.processErrorLInes = processErrorLInes;
	}

	public static int getProcessBytes() {
		return processBytes;
	}

	public static void setProcessBytes(int processBytes) {
		DBWriter.processBytes = processBytes;
	}

	public static int getProcessLines() {
		return processLines;
	}

	public static void setProcessLines(int processLines) {
		DBWriter.processLines = processLines;
	}

	public int getErrCount() {
		return errCount;
	}

	public void setErrCount(int errCount) {
		this.errCount = errCount;
	}

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
			processLines = 0;
			
			if(conn == null)
			conn = DBCPPoolManager.getConnection(poolName);
			
			String strTarCopyOptions = ConfigInfo.TAR_COPY_OPTIONS;
			String strCopyOptions = "";
			if(strTarCopyOptions != null && !strTarCopyOptions.equals("")) strCopyOptions = " " + strTarCopyOptions;
			
			CopyManager copyManager = new CopyManager(((DelegatingConnection<?>)conn).getInnermostDelegate().unwrap(BaseConnection.class));
			copyIn = copyManager.copyIn("COPY " + ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME + "." + table_nm + " FROM STDIN" + strCopyOptions);

			byte[] bytes = (lineStr).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET);
			copyIn.writeToCopy(bytes, 0, bytes.length);
			processBytes +=bytes.length;
			processLines += copyIn.endCopy();
			
			conn.commit();
			
		} catch (Exception e) {
			try {	
				errCount += 1;
				processErrorLInes = errCount;
				String strErrLine = StrUtil.strGetLine(e.toString());
				int intErrLine = -1;
				if(strErrLine != null && !strErrLine.equals("")) {
					intErrLine = Integer.parseInt(strErrLine);
				}
				System.out.println("intErrLine : " + intErrLine);
				
				
				//if (copyIn != null) copyIn.cancelCopy();	
				conn.rollback();
				
				//call back
				errDataHandling(lineStr, table_nm, intErrLine);
	
			}catch(Exception ee){
				//throw ee;
				System.out.println(ee.toString());
			}
			

		}finally{
			try {
				//conn.commit();
				if(conn != null) {
					conn.close();
					conn = null;
				}
			}catch(Exception e){
			}
			System.out.println("[" + table_nm + "] successByteCnt : " + processBytes);
			System.out.println("[" + table_nm + "] InsertCnt : " + processLines);
		}
				
	}
	
	/**
	 * Err Handling CallBack Function
	 * @param lineStr
	 * @param table_nm
	 * @param intErrLine
	 * @throws Exception
	 */
	private void errDataHandling(String lineStr, String table_nm, int intErrLine) throws Exception {
		String strOrgString = lineStr;
		String arrDelString[] = strOrgString.split(Constant.R);
		String strDelString = arrDelString[intErrLine-1];
		
		String strTransString = strOrgString.replaceAll(strDelString + Constant.R , "");
		
		errDataFileWrite(strDelString, table_nm, intErrLine);
		
		if(!strTransString.equals("")) {
			DBWrite(strTransString, table_nm);
		}
	}
	
	private void errDataFileWrite(String strErrLine, String tableName, int intErrLine) throws Exception {
		if(ConfigInfo.TAR_TABLE_BAD) {
			FileWriter fileWriter = new FileWriter(tableName);
			fileWriter.badFileCreater(ConfigInfo.OUTPUT_DIRECTORY + tableName + ".bad");
			fileWriter.badFileWrite(strErrLine);
			LogUtils.debug("[Err Line Skip] ErrLine : " + intErrLine + " ErrData : " + strErrLine, DBWriter.class);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String str = "ERROR:  duplicate key value violates unique constraint \"test_pkey\" DETAIL:  Key (id)=(5) already exists. CONTEXT:  COPY test, line 61";
		
		int idx = str.indexOf("line");
		
		String strLine = str.substring(idx+5);
		
		System.out.println(" line:" + strLine);
	}

}
