package com.k4m.experdb.db2pg.writer;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.http.impl.io.SocketOutputBuffer;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.common.StrUtil;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.unload.ExecuteDataTransfer;

public class DBWriter {
	static MsgCode msgCode = new MsgCode();
	//public static DBConfigInfo TAR_DB_CONFIG = new DBConfigInfo();
	
	private CopyIn copyIn = null;
	private Connection conn;
	private long processBytes = 0;
	private long processLines = 0;
	private long processErrorLInes = 0;
	private int errLine = -1;
	private int errCount = 0;
	
	boolean ContaintPool;
	private String poolName = "";
	
	
	public long getProcessErrorLInes() {
		return processErrorLInes;
	}

	public void setProcessErrorLInes(long processErrorLInes) {
		this.processErrorLInes = processErrorLInes;
	}

	public long getProcessBytes() {
		return processBytes;
	}

	public void setProcessBytes(long processBytes) {
		this.processBytes = processBytes;
	}

	public long getProcessLines() {
		return processLines;
	}

	public void setProcessLines(long processLines) {
		this.processLines = processLines;
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
		this.processLines = 0;
		this.processBytes = 0;
		this.errCount = 0;
		try {
			conn = DBCPPoolManager.getConnection(poolName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void DBCPPoolManagerConn() {
	}

	public void DBWrite(String lineStr, String table_nm) throws Exception {
		try {
			
			//System.out.println("@@@@ lineStr : " + lineStr);
			
//			if(conn == null)
//			conn = DBCPPoolManager.getConnection(poolName);
			
			String strCopyOptions = ConfigInfo.TAR_COPY_OPTIONS != null && !ConfigInfo.TAR_COPY_OPTIONS.equals("")
					? " "+ConfigInfo.TAR_COPY_OPTIONS
					: ""
				;
			
			CopyManager copyManager = new CopyManager(((DelegatingConnection<?>)conn).getInnermostDelegate().unwrap(BaseConnection.class));
			copyIn = copyManager.copyIn("COPY " + ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME + "." + table_nm + " FROM STDIN" + strCopyOptions);

			byte[] bytes = (lineStr).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET);
			copyIn.writeToCopy(bytes, 0, bytes.length);
			processBytes += bytes.length;
			copyIn.flushCopy();
			processLines += copyIn.endCopy();
			conn.commit();
		} catch (Exception e) {
			try {	
				errCount += 1;
				processErrorLInes = errCount;
				
				
				//String strErrLine = StrUtil.strGetLine(e.toString());
				int intErrLine = -1;
				
				intErrLine = getLogErrLine(e.toString());

				//if (copyIn != null) copyIn.cancelCopy();	
				conn.rollback();
				
				writeError(table_nm, e);
				
				if(errCount > ConfigInfo.TAR_LIMIT_ERROR) {
					throw e;
				}
				
				//call back
				errDataHandling(lineStr, table_nm, intErrLine,errCount);
	
			}catch(Exception ee){
				//throw ee;
				LogUtils.error(ee.toString(),DBWriter.class);
			}
			

		}finally{
//			try {
//				conn.commit();
//				if (conn != null ) {
//					if(!conn.isClosed()) {
//						conn.close();
//					}
//				}
//			}catch(Exception e){
//			}
			LogUtils.debug(String.format(msgCode.getCode("C0201"),table_nm,processBytes),DBWriter.class);
			LogUtils.debug(String.format(msgCode.getCode("C0202"),table_nm,processLines),DBWriter.class);
		}
				
	}
	
	public void shutdown() {
		try {
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Err Handling CallBack Function
	 * @param lineStr
	 * @param table_nm
	 * @param intErrLine
	 * @throws Exception
	 */
	private void errDataHandling(String lineStr, String table_nm, int intErrLine, int errCount) throws Exception {
		String strOrgString = lineStr;
		
		//System.out.println("@@@ strOrgString : " + strOrgString);
		String arrDelString[] = strOrgString.split(Constant.R);
		if(intErrLine > 0) {
			String strDelString = arrDelString[intErrLine-1];
			
			//strOrgString = replaceSpecialChar(strOrgString);
			String strTransString = "";
			for(int i=0; i< arrDelString.length; i++) {
				if(!arrDelString[i].equals(strDelString)) {
					strTransString += arrDelString[i] + Constant.R;
				}
			}

			//String strTransString = replaceSpecialChar(strOrgString).replaceAll(replaceSpecialChar(strDelString + Constant.R) , "");
			
			errDataFileWrite(strDelString, table_nm, intErrLine, errCount);
			
			if(!strTransString.equals("")) {
				DBWrite(strTransString, table_nm);
			}
		}
	}
	
	private String replaceSpecialChar(String strContent) throws Exception {

		String regex = ".*[\\[\\][:]\\\\/?[*]].*";
		
		if(strContent.matches(regex)){  // text에 정규식에 있는 문자가 있다면 true 없다면 false 
			//대괄호는 소괄호로
			strContent = strContent.replaceAll("\\[", "\\(");
			strContent = strContent.replaceAll("\\]", "\\)");
			//나머지 특수문자는 제거
			strContent = strContent.replaceAll("[[:]\\\\/?[*]]", "");  
			strContent = strContent.replaceAll("\"", "");  
			strContent = strContent.replaceAll("\\", "");
		} 
		
		return strContent;
	}
	
	private void errDataFileWrite(String strErrLine, String tableName, int intErrLine,int errCount) throws Exception {
		if(ConfigInfo.TAR_TABLE_BAD) {
			FileWriter fileWriter = new FileWriter();
			fileWriter.badFileCreater(ConfigInfo.SRC_FILE_OUTPUT_PATH + tableName + ".bad");
			fileWriter.badFileWrite(strErrLine);
			fileWriter.closeBadFileChannels();
			LogUtils.debug(String.format(msgCode.getCode("C0203"),errCount,intErrLine,strErrLine), DBWriter.class);
		}
	}
	
	private int getLogErrLine(String strErrorText) throws Exception {
		int intErrLine = 0;
		
		String regEx = "line\\s[0-9]*";
		Pattern p = Pattern.compile(regEx); 
		
		Matcher match = p.matcher(strErrorText);
		String strSearch = "";
	    while (match.find()) {
	        strSearch = match.group(0);
	    }
	    
	    if(!strSearch.equals("")) {
	    	String strErrLine = strSearch.replaceAll("[^0-9]", "");
	    	intErrLine = Integer.parseInt(strErrLine);
	    }
		return intErrLine;
	}
	
//	public static void main(String[] args) throws Exception {
//		
//		String str = "ERROR:  duplicate key value violates unique constraint \"test_pkey\" DETAIL:  Key (id)=(5) already exists. CONTEXT:  COPY test, line 61";
//		
//
//		str = " Hint: You need to rebuild PostgreSQL using --with-libxml.Where: COPY jobcandidate,";
//		str +=  " line 1, column resume: ";
//		str +=  "\"<ns:Resume xmlns:ns=\"http://schemas.microsoft.com";
//		str += "/sqlserver/2004/07/adventure-works/Resume\"><ns:Name...";
//		str += " ERROR: unsupported XML feature ";
//		str += "		   Detail: This functionality requires the server to be built with libxml support. ";
//		str += "		   Hint: You need to rebuild PostgreSQL using --with-libxml. ";
//		str += "		   Where: COPY jobcandidate, line 1, column resume: \"<ns:Resume xmlns:ns=\"http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/Resume\"><ns:Name...";
//		str += "		 org.postgresql.util.PSQLException: ERROR: unsupported XML feature ";
//		str += "		   Detail: This functionality requires the server to be built with libxml support. ";
//		str += "		   Hint: You need to rebuild PostgreSQL using --with-libxml. ";
//		str += "		   Where: COPY jobcandidate, line 1, column resume: \"<ns:Resume xmlns:ns=\"http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/Resume\"><ns:Name...";
//		str += "		 ERROR: unsupported XML feature ";
//		str += "		   Detail: This functionality requires the server to be built with libxml support. ";
//		str += "		   Hint: You need to rebuild PostgreSQL using --with-libxml. ";
//		str += "		   Where: COPY jobcandidate, line 1, column resume: \"<ns:Resume xmlns:ns=\"http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/Resume\"><ns:Name...";
//		str += "		 null ";
//		
//		int idx = str.indexOf("line");
//		
//		String strLine = str.substring(idx+5);
//		
//		String regEx = "line\\s[0-9]*";
//		Pattern p = Pattern.compile(regEx); 
//		
//		Matcher match = p.matcher(str);
//		
//		String strSearch = "";
//		
//	    int matchCount = 0;
//	    while (match.find()) {
//	        //System.out.println(matchCount + " : " + match.group(0));
//	        matchCount++;
//	        strSearch = match.group(0);
//
//	    }
//		
//		//System.out.println(" line:" + strSearch);
//		
//		String clean1 = strSearch.replaceAll("[^0-9]", "");
//		//System.out.println(" line: " + clean1);
//	}
//	
	
	private void writeError(String table_nm, Exception e) throws Exception {

		LogUtils.error(
				"\"CharSet:"
				+ ( ConfigInfo.TAR_DB_CONFIG.CHARSET != null && !ConfigInfo.TAR_DB_CONFIG.CHARSET.equals("")
					? DevUtils.classifyString(ConfigInfo.TAR_DB_CONFIG.CHARSET,ConfigInfo.SRC_CLASSIFY_STRING) + "\", \"Table:"
					: "")
				+ table_nm + "\"",ExecuteDataTransfer.class,e);			
	}

}
