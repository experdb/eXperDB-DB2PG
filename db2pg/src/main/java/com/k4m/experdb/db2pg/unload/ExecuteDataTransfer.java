package com.k4m.experdb.db2pg.unload;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.time.StopWatch;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.CreateDbStmt;
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.db.oracle.spatial.geometry.Process;
import com.k4m.experdb.db2pg.writer.DBWriter;
import com.k4m.experdb.db2pg.writer.FileWriter;

import oracle.jdbc.internal.OracleTypes;
import oracle.spatial.geometry.JGeometry;
import test.write.TestFileWriter;

public class ExecuteDataTransfer implements Runnable{
	private String srcPoolName, selectQuery, outputFileName, tableName;
	private int status=1;
	long  rowCnt = 0;
	private boolean success;
	private StringBuffer bf = new StringBuffer();

	private DBConfigInfo dbConfigInfo;

	private TestFileWriter writer;
	//private ByteBuffer byteBuffer;
	private StopWatch stopWatch = new StopWatch();
	
	public ExecuteDataTransfer(String srcPoolName, String selectQuery,String outputFileName,DBConfigInfo dbConfigInfo){
		this.srcPoolName = srcPoolName;
		this.selectQuery = selectQuery;
		this.outputFileName = outputFileName.replace("\"", "");
		this.tableName = DevUtils.classifyString(outputFileName,ConfigInfo.CLASSIFY_STRING);
		this.outputFileName = ConfigInfo.OUTPUT_DIRECTORY
								+ DevUtils.classifyString(outputFileName,ConfigInfo.CLASSIFY_STRING).replace("$", "-")+".sql";
		this.dbConfigInfo = dbConfigInfo;

		//this.byteBuffer = ByteBuffer.allocateDirect(ConfigInfo.BUFFER_SIZE);
		this.success = true;
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

	public boolean isSuccess() {
		return success;
	}


	
	public long getRowCnt() {
		return rowCnt;
	}
	
	
	private void execTruncTable(String poolName, String strTableName) throws Exception {
		PreparedStatement psmt = null;
		
		Connection conn = DBCPPoolManager.getConnection(poolName);
		
		try {
			String sql =  CreateDbStmt.GetTruncateTblDDL(DBCPPoolManager.getConfigInfo(srcPoolName).DB_TYPE, ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME, strTableName);
		
			psmt = conn.prepareStatement(sql);
			
			psmt.execute();
		} catch(Exception e) {
			
		} finally {
			CloseConn(conn, psmt);
		}
		
	}

	@Override
	public void run(){
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		
		Connection SrcConn = null;
		PreparedStatement preSrcStmt = null;
		FileWriter fileWriter = null;
		DBWriter dbWriter = null;
		
		try {
			stopWatch.start();
			LogUtils.info(String.format("%s : %s", this.tableName, selectQuery),ExecuteQuery.class);
			
			SrcConn = DBCPPoolManager.getConnection(srcPoolName);
			preSrcStmt = SrcConn.prepareStatement(selectQuery);
			
			if(ConfigInfo.SRC_ROWNUM>-1) {
				preSrcStmt.setMaxRows(ConfigInfo.SRC_ROWNUM);
			}
			
			preSrcStmt.setFetchSize(ConfigInfo.STATEMENT_FETCH_SIZE);
        	ResultSet rs = preSrcStmt.executeQuery();
        	
        	List<String> columnNames = new ArrayList<String>();
        	ResultSetMetaData rsmd = rs.getMetaData();	
     
        	for(int i=1;i<=rsmd.getColumnCount();i++) {
        		columnNames.add(rsmd.getColumnName(i));
        	}
        	
        	LogUtils.debug(String.format("[%s-CREATE_PIPE_LINE]",this.tableName),ExecuteQuery.class);
        	LogUtils.debug(String.format("[%s-CREATE_BUFFEREDOUTPUTSTREAM]",this.tableName),ExecuteQuery.class);
        	LogUtils.debug("[START_FETCH_DATA]" + outputFileName,ExecuteQuery.class);
        	
        	if (ConfigInfo.TRUNCATE) {
        		execTruncTable(Constant.POOLNAME.TARGET.name(), this.tableName);
        	}

			if(ConfigInfo.DB_WRITER_MODE) {
				dbWriter = new DBWriter(Constant.POOLNAME.TARGET.name());
			}
			
			if(ConfigInfo.FILE_WRITER_MODE) {
				fileWriter = new FileWriter(this.tableName);
			}
        	
        	
			
        	while (rs.next()){

        		for (int i = 1; i <= rsmd.getColumnCount(); i++) {	
        			int type = rsmd.getColumnType(i);
        			
        			bf.append(ConvertDataToString(SrcConn,type, rs, i));
        			
        			if (i != rsmd.getColumnCount()) {
        				bf.append("\t");
        			}
        		}
        		
        		bf.append(Constant.R);
        		rowCnt += 1;
        		
        		ByteBuffer byteBuffer = ByteBuffer.allocateDirect(ConfigInfo.BUFFER_SIZE);

        		if(rowCnt % ConfigInfo.STATEMENT_FETCH_SIZE == 0 && bf.length() > byteBuffer.capacity()) {
        			
        			if(ConfigInfo.DB_WRITER_MODE) {
        				dbWriter.DBWrite(bf.toString(), this.tableName);
        			} else if(ConfigInfo.FILE_WRITER_MODE) {
        				fileWriter.dataWriteToFile(bf.toString(), this.tableName);
        			}
        			
        			bf.setLength(0);
        		}
        	}
        	rs.close();
        	
        	if (bf.length() != 0){
        		
    			if(ConfigInfo.DB_WRITER_MODE) {
    				dbWriter.DBWrite(bf.toString(), this.tableName);
    			} else if(ConfigInfo.FILE_WRITER_MODE) {
    				fileWriter.dataWriteToFile(bf.toString(), this.tableName);
    			}
        	}
			
        	

        	stopWatch.stop();
        	LogUtils.debug("[ELAPSED_TIME] "+tableName+" " + stopWatch.getTime()+"ms",ExecuteQuery.class);
        	
		} catch(Exception e) {
			this.success = false;

				try {
					writeError(outputFileName, e);
				} catch (Exception e1) {
				}

		} finally {
			try {
				fileWriter.closeFileChannels();
			} catch (Exception e) {
			}
			
			CloseConn(SrcConn, preSrcStmt);
			status = 0;
			LogUtils.debug("[END_FETCH_DATA]" + outputFileName,ExecuteQuery.class);
			LogUtils.info("COMPLETE UNLOAD (TABLE_NAME : " +tableName + ", ROWNUM : " + rowCnt + ") !!!",ExecuteQuery.class);
		}
	}
	
	private void writeError(String errFileName, Exception e) throws Exception {
		File output_file = new File(errFileName+".error");

		PrintStream ps = new PrintStream(output_file);
		ps.print("ERROR :\n");
		e.printStackTrace(ps);
		ps.print("SQL :\n");
		ps.print(selectQuery);
		ps.close();

		LogUtils.error(
				"\""
				+ ( ConfigInfo.TAR_DB_CONFIG.CHARSET != null && !ConfigInfo.TAR_DB_CONFIG.CHARSET.equals("")
					? DevUtils.classifyString(ConfigInfo.TAR_DB_CONFIG.CHARSET,ConfigInfo.CLASSIFY_STRING) + "\".\""
					: "")
				+ this.tableName + "\"",ExecuteQuery.class,e);
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
					
					BufferedReader reader = null;
					str = null;
					char[] buffer = null;
					int n = 0;
					
					if(clob.length() < 32766 && !ConfigInfo.SRC_IS_ASCII) { 
						str = rs.getString(index);
						return str == null ? "\\N" : DevUtils.replaceEach(str, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
					} else {
						if ( ConfigInfo.SRC_IS_ASCII ) {
							reader = new BufferedReader(new InputStreamReader(clob.getAsciiStream(),ConfigInfo.SRC_DB_CONFIG.CHARSET));
						} else {
							reader = new BufferedReader(clob.getCharacterStream());
						}
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
			case Types.BLOB:
				blob = rs.getBlob(index);
				
				if (blob == null){
					return "\\N";
				} else {
					byte[] buffer = new byte[ConfigInfo.BUFFER_SIZE];
					int len = 0;
					in = blob.getBinaryStream();
					if (blob != null){
						ByteArrayOutputStream buffeOutr = new ByteArrayOutputStream();
						if (blob.length() < ConfigInfo.BUFFER_SIZE) {		
							len = in.read(buffer);
							if(len > -1)
								buffeOutr.write(buffer, 0, len);
							buffeOutr.flush();
							bf.append("\\\\x");
							bf.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
						} else {
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
						}
						buffeOutr.close();	
					}
					in.close();
				
					return "";	
				}
			case Types.VARBINARY:
				bytes = rs.getBytes(index);
				return bytes == null ? "\\N" : bytes.toString();
			case Types.LONGVARBINARY:
				in = rs.getBinaryStream(index);
				if(in == null) {
					return "\\N";
				} else {
					byte[] buffer = new byte[ConfigInfo.BUFFER_SIZE];
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
	
	private void CloseConn(Connection conn, PreparedStatement pStmt) {
		try{
			if(pStmt != null) {
				pStmt.close();
			}
			if (conn != null && !conn.isClosed()) {
				conn.commit();
				conn.close();
				conn = null;
			}	
		}catch(Exception e){
			LogUtils.error(e.getMessage(),ExecuteQuery.class,e);
		}
	}
	
	private void CloseConn(Connection conn, Statement stmt) {
		try{
			if(stmt != null) {
				stmt.close();
			}
			if (conn != null && !conn.isClosed()) {
				conn.commit();
				conn.close();
				conn = null;
			}	
		}catch(Exception e){
			LogUtils.error(e.getMessage(),ExecuteQuery.class,e);
		}
	}
	
	

}
