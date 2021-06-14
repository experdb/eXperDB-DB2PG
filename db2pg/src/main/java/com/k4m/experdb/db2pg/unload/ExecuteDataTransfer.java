package com.k4m.experdb.db2pg.unload;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.time.StopWatch;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.CreateDbStmt;
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.db.oracle.spatial.geometry.Process;
import com.k4m.experdb.db2pg.rebuild.TargetPgDDL;
import com.k4m.experdb.db2pg.writer.DBWriter;
import com.k4m.experdb.db2pg.writer.FileWriter;

import oracle.jdbc.internal.OracleTypes;
import oracle.spatial.geometry.JGeometry;

public class ExecuteDataTransfer implements Runnable{
	static MsgCode msgCode = new MsgCode();
	private String srcPoolName, selectQuery, outputFileName, tableName;
	private long migTime;
	private long startTime;
	private long endTime;
	private long processBytes = 0;
	private long processLines = 0;
	private long processErrorLInes = 0;
	private int total = 0;
	private int cnt = 0;
	
	private int status=1;
	long rowCnt = 0;
	private boolean success;
	private StringBuffer bf = new StringBuffer();
	List<String> columnNames;

	private DBConfigInfo dbConfigInfo;


	//private StopWatch stopWatch = new StopWatch();
	
	public ExecuteDataTransfer(String srcPoolName, String selectQuery,String outputFileName,DBConfigInfo dbConfigInfo){
		this.srcPoolName = srcPoolName;
		this.selectQuery = selectQuery;
		this.outputFileName = outputFileName.replace("\"", "");
		this.outputFileName = ConfigInfo.SRC_FILE_OUTPUT_PATH+"data/"
								+ DevUtils.classifyString(outputFileName,ConfigInfo.SRC_CLASSIFY_STRING).replace("$", "-")+".sql";
		this.tableName = DevUtils.classifyString(outputFileName,ConfigInfo.SRC_CLASSIFY_STRING);
		this.dbConfigInfo = dbConfigInfo;

		//this.byteBuffer = ByteBuffer.allocateDirect(ConfigInfo.SRC_BUFFER_SIZE);
		this.success = true;
	}
	
	public ExecuteDataTransfer() {
	}
	
	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public int getCnt() {
		return cnt;
	}

	public void setCnt(int cnt) {
		this.cnt = cnt;
	}
	
	public String getSrcPoolName() {
		return srcPoolName;
	}

	public String getSelectQuery() {
		return selectQuery;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public String getTableName() {
		return tableName;
	}

	public int getStatus() {
		return status;
	}
	
	public Long getMigTime() {
		return migTime;
	}

	public boolean isSuccess() {
		return success;
	}

	public long getRowCnt() {
		return rowCnt;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public long getEndTime() {
		return endTime;
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

	public long getProcessErrorLInes() {
		return processErrorLInes;
	}

	public void setProcessErrorLInes(long processErrorLInes) {
		this.processErrorLInes = processErrorLInes;
	}

	private void execTruncTable(String poolName, String strTableName) throws Exception {
		PreparedStatement psmt = null;
		
		Connection conn = DBCPPoolManager.getConnection(poolName);
		
		try {

			String sql =  CreateDbStmt.GetTruncateTblDDL(DBCPPoolManager.getConfigInfo(poolName).DB_TYPE, ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME, strTableName);
		
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

	@Override
	public void run(){
		//ExecutorService executorService = Executors.newFixedThreadPool(1);
		
		Connection SrcConn = null;
		PreparedStatement preSrcStmt = null;
		ResultSet rs = null;
		FileWriter fileWriter = null;
		DBWriter dbWriter = null;
		
		try {
			//stopWatch.start();
			
			LogUtils.debug(String.format("%s : %s", this.tableName, selectQuery),ExecuteDataTransfer.class);
			
			SrcConn = DBCPPoolManager.getConnection(srcPoolName);
			preSrcStmt = SrcConn.prepareStatement(selectQuery);
			
			if(ConfigInfo.SRC_ROWS_EXPORT>-1) {
				preSrcStmt.setMaxRows(ConfigInfo.SRC_ROWS_EXPORT);
			}
			
			preSrcStmt.setFetchSize(ConfigInfo.SRC_STATEMENT_FETCH_SIZE);
        	rs = preSrcStmt.executeQuery();
        	
        	columnNames = new ArrayList<String>();
        	ResultSetMetaData rsmd = rs.getMetaData();	
     
        	for(int i=1;i<=rsmd.getColumnCount();i++) {
        		columnNames.add(rsmd.getColumnName(i));
        	}
        	
        	//LogUtils.debug(String.format(msgCode.getCode("C0131"),this.tableName),ExecuteDataTransfer.class);
        	LogUtils.debug(String.format(msgCode.getCode("C0132"),this.tableName),ExecuteDataTransfer.class);
        	
			if(ConfigInfo.DB_WRITER_MODE) {
				LogUtils.debug(String.format(msgCode.getCode("C0131"),this.tableName),ExecuteDataTransfer.class);
				if (ConfigInfo.TAR_TRUNCATE) {
					execTruncTable(Constant.POOLNAME.TARGET.name(), this.tableName);
				}
				dbWriter = new DBWriter(Constant.POOLNAME.TARGET.name());
			}
			if(ConfigInfo.FILE_WRITER_MODE) {
				LogUtils.debug(String.format(msgCode.getCode("C0133"),outputFileName),ExecuteDataTransfer.class);
				fileWriter = new FileWriter(this.tableName);
			}
			
			this.startTime = System.currentTimeMillis();
			
			int intErrCnt = 0;
			
        	while (rs.next()){
        		rowCnt += 1;
        		for (int i = 1; i <= rsmd.getColumnCount(); i++) {	
        			int type = rsmd.getColumnType(i);
        			///System.out.println(ConvertDataToString(SrcConn,type, rs, i));
        			
        			bf.append(ConvertDataToString(SrcConn,type, rs, i));
        			
        			if (i != rsmd.getColumnCount()) {
        				bf.append("\t");
        			}
        		}
        		bf.append(Constant.R);
        		
        		//System.out.println("length : " + bf.length() + " " + bf.toString());

        		int intSRC_BUFFER_SIZE = ConfigInfo.SRC_BUFFER_SIZE;
        		//ByteBuffer byteBuffer = ByteBuffer.allocateDirect(ConfigInfo.SRC_BUFFER_SIZE);
        		

        		if((rowCnt % ConfigInfo.SRC_COPY_SEGMENT_SIZE == 0) || (bf.length() > intSRC_BUFFER_SIZE)) {
        			if(ConfigInfo.DB_WRITER_MODE) {
        					dbWriterN(dbWriter);
        					intErrCnt += dbWriter.getErrCount();
        			} 
        			
        			if(ConfigInfo.FILE_WRITER_MODE) {
        				fileWriter.dataWriteToFile(bf.toString(), this.tableName);
        			}
        			
        			bf.setLength(0);
        			
        			if(intErrCnt > ConfigInfo.TAR_LIMIT_ERROR) {
        				break;
        			}
        		}
        		
        	}
        	
        	if (bf.length() != 0){
	    		if(ConfigInfo.DB_WRITER_MODE) {
	       			if(!(intErrCnt > ConfigInfo.TAR_LIMIT_ERROR)) {
	    				dbWriter.DBWrite(bf.toString(), this.tableName);
	    			}
	    		} 
   
    			if(ConfigInfo.FILE_WRITER_MODE) {
    				fileWriter.dataWriteToFile(bf.toString(), this.tableName);
    			}
        	}
			
        	if(intErrCnt > 0) {
        		this.success = false;
        	}

        	this.endTime = System.currentTimeMillis();
        	this.migTime = (this.endTime - this.startTime)/1000;
        	
        	//stopWatch.stop();
        	//this.migTime = stopWatch.getTime();
        	LogUtils.debug(String.format(msgCode.getCode("C0134"),tableName,this.migTime),ExecuteDataTransfer.class);
		} catch(Exception e) {
			this.success = false;

				try {
					writeError(outputFileName, e);
				} catch (Exception e1) {
				}
				LogUtils.error("EXCEPTION!!!!", ExecuteDataTransfer.class,e);
		} finally {
			try {
				if(ConfigInfo.FILE_WRITER_MODE) fileWriter.closeFileChannels(this.tableName);
				
				if(ConfigInfo.DB_WRITER_MODE) {
					LogUtils.info(String.format(msgCode.getCode("C0135"),tableName, dbWriter.getProcessLines(), dbWriter.getProcessBytes(), dbWriter.getProcessErrorLInes(),this.migTime),ExecuteQuery.class);
					if(dbWriter.getProcessErrorLInes() > 0) {
						this.success = false;
					}
					
					processBytes = dbWriter.getProcessBytes();
					processLines = dbWriter.getProcessLines();
					processErrorLInes = dbWriter.getProcessErrorLInes();
					
					//dbWriter Closed
					dbWriter.shutdown();
				}
				if(rs != null) rs.close();
			} catch (Exception e) {
			}
			

			CloseConn(SrcConn, preSrcStmt);
			status = 0;
			if(ConfigInfo.FILE_WRITER_MODE) {
				LogUtils.debug(String.format(msgCode.getCode("C0136"),outputFileName),ExecuteDataTransfer.class);
			}
			LogUtils.info(String.format(msgCode.getCode("C0137"),tableName,rowCnt),ExecuteDataTransfer.class);
			try {
				progressFileWrite(total+","+cnt+","+tableName+","+rowCnt);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void dbWriterN(DBWriter dbWriter) throws Exception {
		try {
			dbWriter.DBWrite(bf.toString(), this.tableName);
		} catch (Exception e) {
			throw e;
		}
	}
	
	private void writeError(String errFileName, Exception e) throws Exception {
		File output_file = new File(ConfigInfo.SRC_FILE_OUTPUT_PATH+errFileName+".error");

		PrintStream ps = new PrintStream(output_file);
		ps.print("ERROR :\n");
		e.printStackTrace(ps);
		ps.print("SQL :\n");
		ps.print(selectQuery);
		ps.close();

		LogUtils.error(
				"\"CharSet:"
				+ ( ConfigInfo.TAR_DB_CONFIG.CHARSET != null && !ConfigInfo.TAR_DB_CONFIG.CHARSET.equals("")
					? DevUtils.classifyString(ConfigInfo.TAR_DB_CONFIG.CHARSET,ConfigInfo.SRC_CLASSIFY_STRING) + "\", \"Table:"
					: "")
				+ this.tableName + "\"",ExecuteDataTransfer.class,e);
	}
	
	private String ConvertDataToString(Connection SrcConn,int columnType, ResultSet rs, int index) throws SQLException, Exception {
		try {
			Boolean bool = null;
			String str = null;
			BigDecimal bigDecimal = null;
			Date date = null;
			Time time = null;
			Timestamp timestamp = null;
			Clob clob = null;
			Blob blob = null;
			byte[] bytes = null;
			InputStream in = null;
			SQLXML xml = null;
			Object obj = null;
			NClob nclob = null;
			
			switch (columnType){
			case Types.BIT:
				bool = rs.getBoolean(index);
				return bool == null ? "\\N" : bool.toString();
			case Types.VARCHAR:  case Types.LONGVARCHAR:  case Types.CHAR: 
				if(ConfigInfo.SRC_IS_ASCII) {
					byte[] b = rs.getBytes(index);
					
					if ( b != null) str = new String(b, ConfigInfo.SRC_DB_CONFIG.CHARSET);
					else str = null;
					
				} else {
					str = rs.getString(index);
				}
				return str == null ? "\\N" : DevUtils.replaceEach(str, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
			case Types.NVARCHAR: case Types.LONGNVARCHAR: case Types.NCHAR:
				str = rs.getString(index);
				return str == null ? "\\N" : DevUtils.replaceEach(str, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
			case Types.NUMERIC:
				bigDecimal = rs.getBigDecimal(index);
				return bigDecimal == null ? "\\N" : bigDecimal.toString();
			case Types.TINYINT: case Types.SMALLINT: case Types.INTEGER: case Types.BIGINT:
			case Types.FLOAT: case Types.REAL: case Types.DOUBLE: case Types.DECIMAL:
				str = rs.getString(index);
				return str == null ? "\\N" : DevUtils.replaceEach(str, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
			case Types.DATE:
				date = rs.getDate(index);
				return date == null ? "\\N" : date.toString();
			case Types.TIME:
				time = rs.getTime(index);
				return time == null ? "\\N" : time.toString();
			case Types.TIMESTAMP:
				timestamp = rs.getTimestamp(index); 
				return timestamp == null ? "\\N" : timestamp.toString();
			case Types.CLOB:
				clob = rs.getClob(index);
				
				if (clob != null) {
					
					
					str = null;

					if(clob.length() < 32766 && !ConfigInfo.SRC_IS_ASCII) { 
						str = rs.getString(index);
						return str == null ? "\\N" : DevUtils.replaceEach(str, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
					} else {
						BufferedReader reader = null;
						
						if ( ConfigInfo.SRC_IS_ASCII ) {
							reader = new BufferedReader(new InputStreamReader(clob.getAsciiStream(),ConfigInfo.SRC_DB_CONFIG.CHARSET));
						} else {
							reader = new BufferedReader(clob.getCharacterStream());
						}
						
						char[] buffer = new char[ ConfigInfo.SRC_LOB_BUFFER_SIZE ];
						
						int n = 0;
/*						StringBuffer sb = new StringBuffer();
						while((n = reader.read(buffer)) != -1){
							sb.append(buffer, 0, n);				
						}*/
						
						while((n = reader.read(buffer)) != -1) {
							String s = DevUtils.replaceEach(new String(Arrays.copyOfRange(buffer, 0, n)), DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
							bf.append(s);

						}
						//String s = DevUtils.replaceEach(sb.toString(), DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
						//bf.append(s);
						
						reader.close();
						return "";
					}
				}
				return "\\N";
			case Types.BLOB:
				blob = rs.getBlob(index);
				
				if (blob == null){
					return "\\N";
				} else {
					
					int len = 0;
					in = blob.getBinaryStream();
					
					//System.out.println("@@@@@@@@@@ ===> " + in.available());
					
					byte[] buffer = new byte[ConfigInfo.SRC_LOB_BUFFER_SIZE];
					
					if (blob != null){
						ByteArrayOutputStream buffeOutr = new ByteArrayOutputStream();
						bf.append("\\\\x");
						if (blob.length() < ConfigInfo.SRC_LOB_BUFFER_SIZE){								
							len = in.read(buffer);
							if(len > 0)
								buffeOutr.write(buffer, 0, len);
							buffeOutr.flush();
							bf.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));								
						}else{							
							while((len = in.read(buffer))!= -1){																		
									buffeOutr.write(buffer, 0, len);
									buffeOutr.flush();
									bf.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
				        			buffeOutr.reset();					        			
							}							
						}
						
						buffeOutr.close();		
					}
					in.close();
				
					return "";	
				}
			/*case Types.VARBINARY:
				bytes = rs.getBytes(index);
				return bytes == null ? "\\N" : bytes.toString();*/
			case Types.LONGVARBINARY: case Types.VARBINARY : case Types.BINARY :
				in = rs.getBinaryStream(index);
				if(in == null) {
					return "\\N";
				} else {
					byte[] buffer = new byte[ConfigInfo.SRC_BUFFER_SIZE];
					int len = 0;
					
					ByteArrayOutputStream buffeOutr = new ByteArrayOutputStream();
					if(bf.length()>0) {
						//divideProcessing();
					}
        			
					bf.append("\\\\x");
					while((len = in.read(buffer))!= -1) {
						buffeOutr.write(buffer, 0, len);
						buffeOutr.flush();
						bf.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
	        			buffeOutr.reset();

					}
					
					buffeOutr.close();	
					in.close();
					return "";
				}
				
			case OracleTypes.OPAQUE: case Types.SQLXML:
				xml = rs.getSQLXML(index); 
				return xml == null ? "\\N" : xml.toString();
			case Types.STRUCT:  
				obj = rs.getObject(index);
				if(obj == null)	return "\\N";
				
				//ORACLE STRUCT
				if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.ORA)) {
					if(obj instanceof oracle.sql.STRUCT) {
						oracle.sql.STRUCT struct = (oracle.sql.STRUCT)obj;
						if(struct.getSQLTypeName().equals("MDSYS.SDO_GEOMETRY")) {
							JGeometry jgeo = JGeometry.load(struct.getBytes());
							String r = Process.parseSdoGeometry(SrcConn, jgeo);
							return r!=null?r:"\\N";
						}
					}
				} else {
					//TYPE NOT CONVERT
				}
				return "\\N";
			case Types.NCLOB:
				nclob = rs.getNClob(index);
				
				if (nclob != null) {
					BufferedReader reader = null;
					str = null;
					char[] buffer = null;
					int n = 0;
					
					if(nclob.length() < 32766 && !ConfigInfo.SRC_IS_ASCII) { 
						str = rs.getString(index);
						return str == null ? "\\N" : DevUtils.replaceEach(str, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
					} else {
						reader = new BufferedReader(nclob.getCharacterStream());
						buffer = new char[ 4 * 1024 ];
						
						if(bf.length()>0) {
							//divideProcessing();
						}
						
						
						while((n = reader.read(buffer)) != -1) {
							String s = DevUtils.replaceEach(new String(Arrays.copyOfRange(buffer, 0, n)), DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
							bf.append(s);

						}
						reader.close();
						return "";
					}
				}
				return "\\N";
			case Types.NULL:
				return "\\N";
			default : // Other Types
				obj = rs.getObject(index);
				return obj == null ? "\\N" : obj.toString();
			}
		} catch(Exception e){
			throw e;
		}
	}
	
	private void CloseConn(Connection conn, PreparedStatement pStmt, ResultSet rs) {
		try{
			if(rs != null) rs.close();
			if(pStmt != null) pStmt.close();
			if (conn != null && !conn.isClosed()) {
				conn.close();
				conn = null;
			}	
		}catch(Exception e){
			LogUtils.error(e.getMessage(),ExecuteDataTransfer.class,e);
		}
	}
	
	private void CloseConn(Connection conn, Statement stmt, ResultSet rs) {
		try{
			if(stmt != null) {
				stmt.close();
			}
			if(rs != null) rs.close();
			if (conn != null && !conn.isClosed()) {
				conn.close();
				conn = null;
			}	
		}catch(Exception e){
			LogUtils.error(e.getMessage(),ExecuteDataTransfer.class,e);
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
			LogUtils.error(e.getMessage(),ExecuteDataTransfer.class,e);
		}
	}
	
	private void progressFileWrite(String proData) throws Exception {
		FileWriter fileWriter = new FileWriter();
		fileWriter.progressFile(ConfigInfo.SRC_FILE_OUTPUT_PATH + "result/progress.txt");
		fileWriter.progressFileWrite(proData);
		fileWriter.closeProgressFileChannels();
	}

}
