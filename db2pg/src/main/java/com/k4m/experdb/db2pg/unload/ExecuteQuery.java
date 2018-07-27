package com.k4m.experdb.db2pg.unload;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
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
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.db.oracle.spatial.geometry.Process;

import oracle.jdbc.internal.OracleTypes;
import oracle.spatial.geometry.JGeometry;
import test.write.TestFileWriter;

public class ExecuteQuery implements Runnable{
	private String srcPoolName, selectQuery, outputFileName, tableName;
	private int status=1;
	long  rowCnt = 0;
	private boolean success;
	private StringBuffer stringBuffer = null;
	private GatheringByteChannel outChannel;
	private DBConfigInfo dbConfigInfo;
	/** tmp Class ( volatilizable ) 
	 * maybe this class is removed as soon.*/
	private TestFileWriter writer;
	private ByteBuffer byteBuffer;
	private StopWatch stopWatch = new StopWatch();
	
	public ExecuteQuery(String srcPoolName, String selectQuery,String outputFileName,DBConfigInfo dbConfigInfo){
		this.srcPoolName = srcPoolName;
		this.selectQuery = selectQuery;
		this.outputFileName = outputFileName.replace("\"", "");
		this.tableName = DevUtils.classifyString(outputFileName,ConfigInfo.CLASSIFY_STRING);
		this.outputFileName = ConfigInfo.OUTPUT_DIRECTORY
								+ DevUtils.classifyString(outputFileName,ConfigInfo.CLASSIFY_STRING).replace("$", "-")+".sql";
		this.dbConfigInfo = dbConfigInfo;
		this.byteBuffer = ByteBuffer.allocateDirect(ConfigInfo.BUFFER_SIZE);
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

	public StringBuffer getStringBuffer() {
		return stringBuffer;
	}

	public GatheringByteChannel getOutChannel() {
		return outChannel;
	}
	
	public long getRowCnt() {
		return rowCnt;
	}
	

	@Override
	public void run(){
		Connection SrcConn = null;
		PreparedStatement preSrcStmt = null;
		
		try {
			stopWatch.start();
			LogUtils.info(String.format("%s : %s", this.tableName, selectQuery),ExecuteQuery.class);
			SrcConn = DBCPPoolManager.getConnection(srcPoolName);
			preSrcStmt = SrcConn.prepareStatement(selectQuery);
			if(ConfigInfo.SRC_ROWNUM>-1)
				preSrcStmt.setMaxRows(ConfigInfo.SRC_ROWNUM);
			preSrcStmt.setFetchSize(ConfigInfo.STATEMENT_FETCH_SIZE);
        	ResultSet rs = preSrcStmt.executeQuery();
        	ResultSetMetaData rsmd = rs.getMetaData();	
        	List<String> columnNames = new ArrayList<String>();
        	for(int i=1;i<=rsmd.getColumnCount();i++)
        		columnNames.add(rsmd.getColumnName(i));
        	
        	LogUtils.debug(String.format("[%s-CREATE_PIPE_LINE]",this.tableName),ExecuteQuery.class);

        	File output_file = new File(outputFileName);
        	FileOutputStream fos = new FileOutputStream(output_file);
        	outChannel = fos.getChannel();
    		
        	LogUtils.debug(String.format("[%s-CREATE_BUFFEREDOUTPUTSTREAM]",this.tableName),ExecuteQuery.class);
        	LogUtils.debug("[START_FETCH_DATA]" + outputFileName,ExecuteQuery.class);
        	
        	stringBuffer = new StringBuffer("");
        	stringBuffer.append("SET client_encoding TO '");
        	stringBuffer.append(ConfigInfo.TAR_DB_CHARSET);
        	stringBuffer.append("';\n\n");
        	stringBuffer.append("\\set ON_ERROR_STOP OFF\n\n");
        	stringBuffer.append("\\set ON_ERROR_ROLLBACK OFF\n\n");
        	if (ConfigInfo.TRUNCATE) {
            	stringBuffer.append("TRUNCATE TABLE \"");
            	if(ConfigInfo.TAR_SCHEMA != null && !ConfigInfo.TAR_SCHEMA.equals("")) {
            		stringBuffer.append(ConfigInfo.TAR_SCHEMA);
            		stringBuffer.append("\".\"");
            	}
            	stringBuffer.append(DevUtils.classifyString(this.tableName,ConfigInfo.CLASSIFY_STRING));
            	stringBuffer.append("\";\n\n");
            	LogUtils.debug("[ADD_TRUNCATE_COMMAND] " + this.tableName,ExecuteQuery.class);
        	} else {
        		LogUtils.debug("[NO_TRUNCATE_COMMAND] " + this.tableName,ExecuteQuery.class);
        	}
			stringBuffer.append("COPY \"");
        	if(ConfigInfo.TAR_SCHEMA != null && !ConfigInfo.TAR_SCHEMA.equals("")) {
        		stringBuffer.append(ConfigInfo.TAR_SCHEMA);
        		stringBuffer.append("\".\"");
        	}
        	stringBuffer.append(DevUtils.classifyString(this.tableName,ConfigInfo.CLASSIFY_STRING));
        	stringBuffer.append("\" (");
        	for(int i=0; i<columnNames.size(); i++){
        		stringBuffer.append('"');
        		stringBuffer.append(DevUtils.classifyString(columnNames.get(i),ConfigInfo.CLASSIFY_STRING));
        		stringBuffer.append('"');
        		if(i<columnNames.size()-1) stringBuffer.append(',');
        	}
        	stringBuffer.append(") FROM STDIN;\n");
        	divideProcessing();
			
        	while (rs.next()){
        		for (int i = 1; i <= rsmd.getColumnCount(); i++) {	
        			int type = rsmd.getColumnType(i);
        			stringBuffer.append(ConvertDataToString(SrcConn,type, rs, i));
        			if (i != rsmd.getColumnCount()) {
        				stringBuffer.append("\t");
        			}
        		}
        		stringBuffer.append("\n");
        		rowCnt += 1;

        		if(rowCnt % ConfigInfo.STATEMENT_FETCH_SIZE == 0 && stringBuffer.length() > byteBuffer.capacity()) {
        			divideProcessing();
        		}
        	}
        	rs.close();
        	if (stringBuffer.length() != 0){
        		divideProcessing();
        	}
			byteBuffer.put("\\.\n\n".toString().getBytes(ConfigInfo.TAR_DB_CHARSET));
			byteBuffer.flip();
			outChannel.write(byteBuffer);
			byteBuffer.clear();
        	outChannel.close();
        	fos.close();
        	stopWatch.stop();
        	LogUtils.debug("[ELAPSED_TIME] " + stopWatch.getTime()+"ms",ExecuteQuery.class);
        	
		} catch(Exception e) {
			this.success = false;
			File output_file = new File(outputFileName+".error");
			try {
				PrintStream ps = new PrintStream(output_file);
				ps.print("ERROR :\n");
				e.printStackTrace(ps);
				ps.print("SQL :\n");
				ps.print(selectQuery);
				ps.close();
			} catch (FileNotFoundException e1) {
				LogUtils.error("[SQL_ERROR]" + outputFileName+".error",ExecuteQuery.class);
			}
			LogUtils.error(
					"\""
					+ ( ConfigInfo.TAR_SCHEMA != null && !ConfigInfo.TAR_SCHEMA.equals("")
						? DevUtils.classifyString(ConfigInfo.TAR_SCHEMA,ConfigInfo.CLASSIFY_STRING) + "\".\""
						: "")
					+ this.tableName + "\"",ExecuteQuery.class,e);
		} finally {
			CloseConn(SrcConn, preSrcStmt);
			status = 0;
			LogUtils.debug("[END_FETCH_DATA]" + outputFileName,ExecuteQuery.class);
			LogUtils.info("COMPLETE UNLOAD (TABLE_NAME : " +tableName + ", ROWNUM : " + rowCnt + ") !!!",ExecuteQuery.class);
		}
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
					
					if ( b != null) str = new String(b, ConfigInfo.SRC_DB_CHARSET);
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
							reader = new BufferedReader(new InputStreamReader(clob.getAsciiStream(),ConfigInfo.SRC_DB_CHARSET));
						} else {
							reader = new BufferedReader(clob.getCharacterStream());
						}
						buffer = new char[ 4 * 1024 ];
						 
						if(stringBuffer.length()>0) {
							divideProcessing();
						}
						
						
						while((n = reader.read(buffer)) != -1) {
							String s = DevUtils.replaceEach(new String(Arrays.copyOfRange(buffer, 0, n)), DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
							stringBuffer.append(s);
							
							if(stringBuffer.length() > byteBuffer.capacity()) {
								divideProcessing();
							}
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
							stringBuffer.append("\\\\x");
							stringBuffer.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
						} else {
							if(stringBuffer.length()>0) {
								divideProcessing();
							}
    	        			
							stringBuffer.append("\\\\x");
							while((len = in.read(buffer))!= -1) {
								buffeOutr.write(buffer, 0, len);
								buffeOutr.flush();
								stringBuffer.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
			        			buffeOutr.reset();
			        			if(stringBuffer.length() > byteBuffer.capacity()) {
			        				divideProcessing();
			        			}
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
					if(stringBuffer.length()>0) {
						divideProcessing();
					}
        			
					stringBuffer.append("\\\\x");
					while((len = in.read(buffer))!= -1) {
						buffeOutr.write(buffer, 0, len);
						buffeOutr.flush();
						stringBuffer.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
	        			buffeOutr.reset();
	        			if(stringBuffer.length() > byteBuffer.capacity()) {
	        				divideProcessing();
	        			}
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
						
						if(stringBuffer.length()>0) {
							divideProcessing();
						}
						
						
						while((n = reader.read(buffer)) != -1) {
							String s = DevUtils.replaceEach(new String(Arrays.copyOfRange(buffer, 0, n)), DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
							stringBuffer.append(s);
							if(stringBuffer.length() > byteBuffer.capacity()) {
								divideProcessing();
							}
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
	
	private void divideProcessing () throws IOException {
		byte[] bytes = stringBuffer.toString().getBytes(ConfigInfo.TAR_DB_CHARSET);
		
		int bfsCnt = bytes.length/byteBuffer.capacity(); 
		for(int i=0;i<bfsCnt;i++) {
			byteBuffer.put(Arrays.copyOfRange(bytes, i*byteBuffer.capacity(), (i+1)*byteBuffer.capacity()));
			byteBuffer.flip();
			outChannel.write(byteBuffer);
			byteBuffer.clear();
		}
		if(bytes.length%byteBuffer.capacity()>0) {
			byteBuffer.put(Arrays.copyOfRange(bytes, bfsCnt*byteBuffer.capacity(), bytes.length));
			byteBuffer.flip();
			outChannel.write(byteBuffer);
			byteBuffer.clear();
		}
		stringBuffer.setLength(0);
	}
	
	void CloseConn(Connection conn, PreparedStatement pStmt) {
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
	
	void CloseConn(Connection conn, Statement stmt) {
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
