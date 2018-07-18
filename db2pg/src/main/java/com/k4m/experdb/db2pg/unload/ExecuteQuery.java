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

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.db.oracle.spatial.geometry.Process;

import oracle.jdbc.internal.OracleTypes;
import oracle.spatial.geometry.JGeometry;

public class ExecuteQuery implements Runnable{
	private String srcPoolName, selectQuery, outputFileName, tableName;
	private int status=1;
	long  rowCnt = 0;
	private boolean success;
	private StringBuffer bf = null;
	private GatheringByteChannel outChannel;
	private DBConfigInfo dbConfigInfo;
	
	public ExecuteQuery(String srcPoolName, String selectQuery,String outputFileName,DBConfigInfo dbConfigInfo){
		this.srcPoolName = srcPoolName;
		this.selectQuery = selectQuery;
		this.outputFileName = outputFileName.replace("\"", "");
		this.tableName = DevUtils.classifyString(outputFileName,ConfigInfo.CLASSIFY_STRING);
		this.outputFileName = ConfigInfo.OUTPUT_DIRECTORY
								+ DevUtils.classifyString(outputFileName,ConfigInfo.CLASSIFY_STRING).replace("$", "-")+".sql";
		this.dbConfigInfo = dbConfigInfo;
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

	public StringBuffer getBf() {
		return bf;
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
			LogUtils.info(String.format("%s : %s", this.tableName, selectQuery),ExecuteQuery.class);
			SrcConn = DBCPPoolManager.getConnection(srcPoolName);
			preSrcStmt = SrcConn.prepareStatement(selectQuery);
			if(ConfigInfo.SRC_ROWNUM>-1)
				preSrcStmt.setMaxRows(ConfigInfo.SRC_ROWNUM);
			preSrcStmt.setFetchSize(ConfigInfo.SRC_STATEMENT_FETCH_SIZE);
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
        	
        	bf = new StringBuffer("");
        	ByteBuffer bb = ByteBuffer.allocateDirect(ConfigInfo.SRC_IS_ASCII?ConfigInfo.BASIC_BUFFER_SIZE * 2 * 4:ConfigInfo.BASIC_BUFFER_SIZE * 2);
        	bf.append("SET client_encoding TO '");
        	bf.append(ConfigInfo.TAR_DB_CHARSET);
        	bf.append("';\n\n");
        	bf.append("\\set ON_ERROR_STOP OFF\n\n");
        	bf.append("\\set ON_ERROR_ROLLBACK OFF\n\n");
        	if (ConfigInfo.TRUNCATE) {
            	bf.append("TRUNCATE TABLE \"");
            	if(ConfigInfo.TAR_SCHEMA != null && !ConfigInfo.TAR_SCHEMA.equals("")) {
            		bf.append(ConfigInfo.TAR_SCHEMA);
            		bf.append("\".\"");
            	}
            	bf.append(DevUtils.classifyString(this.tableName,ConfigInfo.CLASSIFY_STRING));
            	bf.append("\";\n\n");
            	LogUtils.debug("[ADD_TRUNCATE_COMMAND] " + this.tableName,ExecuteQuery.class);
        	} else {
        		LogUtils.debug("[NO_TRUNCATE_COMMAND] " + this.tableName,ExecuteQuery.class);
        	}
        	bb.put(bf.toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
			bb.flip();
			
			outChannel.write(bb);
			bb.clear();
        	StringBuilder head = new StringBuilder();
        	head.append("COPY \"");
        	if(ConfigInfo.TAR_SCHEMA != null && !ConfigInfo.TAR_SCHEMA.equals("")) {
        		head.append(ConfigInfo.TAR_SCHEMA);
        		head.append("\".\"");
        	}
        	head.append(DevUtils.classifyString(this.tableName,ConfigInfo.CLASSIFY_STRING));
        	head.append("\" (");
        	for(int i=0; i<columnNames.size(); i++){
        		head.append('"');
        		head.append(DevUtils.classifyString(columnNames.get(i),ConfigInfo.CLASSIFY_STRING));
        		head.append('"');
        		if(i<columnNames.size()-1) head.append(',');
        	}
        	head.append(") FROM STDIN;\n");
        	bb.put(head.toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
			bb.flip();
			
			outChannel.write(bb);
			bb.clear();
			
        	bf = new StringBuffer();
        	while (rs.next()){
        		for (int i = 1; i <= rsmd.getColumnCount(); i++) {	
        			int type = rsmd.getColumnType(i);
        			bf.append(ConvertDataToString(SrcConn,type, rs, i));
        			if (i != rsmd.getColumnCount()) {
        				bf.append("\t");
        			}
        		}
        		bf.append("\n");
//        		bf.append(Constant.R);
        		rowCnt += 1;
        		divideProcessing(bf.length(),ConfigInfo.BASIC_BUFFER_SIZE,ConfigInfo.BASIC_BUFFER_SIZE, bf, bb, outChannel, ConfigInfo.FILE_CHARACTERSET);

        		if(rowCnt % ConfigInfo.SRC_TABLE_COPY_SEGMENT_SIZE == 0) {
        			if (bf.length() != 0){
            			bb.put(bf.toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
            			bb.flip();
            			outChannel.write(bb);
            			bb.clear();
            			bf.setLength(0);
                	}
					bb.put("\\.\n\n".toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
		        	bb.put(head.toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
					bb.flip();
					outChannel.write(bb);
					bb.clear();
        		}
        	}
        	
        	if (bf.length() != 0){
    			bb.put(bf.toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
    			bb.flip();
    			outChannel.write(bb);
    			bb.clear();
        	}
			bb.put("\\.\n\n".toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
			bb.flip();
			outChannel.write(bb);
			bb.clear();
        	outChannel.close();
        	fos.close();
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
			
			switch (columnType){
			case Types.BIT:
				Boolean bool = rs.getBoolean(index);
				return bool == null ? "\\N" : bool.toString();
			case Types.VARCHAR: case Types.NVARCHAR: case Types.LONGNVARCHAR: case Types.LONGVARCHAR: 
			case Types.CHAR: case Types.NCHAR:
				String str = null;
				if(ConfigInfo.SRC_IS_ASCII) {
					//str = new String(rs.getString(index).getBytes(ConfigInfo.ASCII_ENCODING),ConfigInfo.FILE_CHARACTERSET);
					byte[] b = rs.getBytes(index);
					
					if ( b != null) str = new String(b, ConfigInfo.SRC_DB_CHARSET);
					else str = null;
					
				} else {
					str = rs.getString(index);
				}
				return str == null ? "\\N" : DevUtils.replaceEach(str, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
			case Types.NUMERIC:
				BigDecimal bigDecimal = rs.getBigDecimal(index);
				return bigDecimal == null ? "\\N" : bigDecimal.toString();
			case Types.TINYINT: case Types.SMALLINT: case Types.INTEGER: case Types.BIGINT:
			case Types.FLOAT: case Types.REAL: case Types.DOUBLE: case Types.DECIMAL:
				String numStr = rs.getString(index);
				return numStr == null ? "\\N" : DevUtils.replaceEach(numStr, DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
			case Types.DATE:
				Date date = rs.getDate(index);
				return date == null ? "\\N" : date.toString();
			case Types.TIME:
				Time time = rs.getTime(index);
				return time == null ? "\\N" : time.toString();
			case Types.TIMESTAMP:
				Timestamp timestamp = rs.getTimestamp(index); 
				return timestamp == null ? "\\N" : timestamp.toString();
			case Types.CLOB:
				Clob clob = rs.getClob(index);
				
				if (clob != null) {
					BufferedReader reader = null;
					if ( ConfigInfo.SRC_IS_ASCII ) {
						reader = new BufferedReader(new InputStreamReader(clob.getAsciiStream(),ConfigInfo.SRC_DB_CHARSET));
					} else {
						reader = new BufferedReader(clob.getCharacterStream());
					}
					char[] buffer = new char[ConfigInfo.CLOB_BUFFER_SIZE];
					int n = 0;
					
					if (ConfigInfo.BASIC_BUFFER_SIZE > clob.length() + bf.length()) { 
						StringBuffer sb = new StringBuffer();
						while((n = reader.read(buffer)) != -1){
							sb.append(buffer, 0, n);
						}
						reader.close();
						return DevUtils.replaceEach(sb.toString(), DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
					} else {
						if(bf.length()>0) {
							ByteBuffer tmpByteBuffer = ByteBuffer.allocateDirect(ConfigInfo.BASIC_BUFFER_SIZE*2);
							divideProcessing(ConfigInfo.BASIC_BUFFER_SIZE, bf, tmpByteBuffer, outChannel, ConfigInfo.FILE_CHARACTERSET);
						}
						
						ByteBuffer bb = ByteBuffer.allocateDirect(buffer.length*4);
						
						while((n = reader.read(buffer)) != -1){
							String s = new String(Arrays.copyOfRange(buffer, 0, n));
							bb.put(s.getBytes(ConfigInfo.FILE_CHARACTERSET));
							bb.flip();
			        		outChannel.write(bb);
			        		bb.clear();
						}
						reader.close();
						return "";
					}
				}
				return "\\N";
			case Types.BLOB:
				Blob blob = rs.getBlob(index);
				
				if (blob == null){
					return "\\N";
				} else {
					byte[] buffer = new byte[ConfigInfo.BLOB_BUFFER_SIZE];
					int len = 0;
					InputStream in = blob.getBinaryStream();
					ByteBuffer bb = ByteBuffer.allocateDirect(ConfigInfo.BLOB_BUFFER_SIZE);
					if (blob != null){
						ByteArrayOutputStream buffeOutr = new ByteArrayOutputStream();
						if (blob.length() < ConfigInfo.BLOB_BUFFER_SIZE) {		
							len = in.read(buffer);
							if(len > -1)
								buffeOutr.write(buffer, 0, len);
							buffeOutr.flush();
							bf.append("\\\\x");
							bf.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
						} else {
							if(bf.length()>0) {
								ByteBuffer tmpByteBuffer = ByteBuffer.allocateDirect(ConfigInfo.BASIC_BUFFER_SIZE*2);
								divideProcessing(ConfigInfo.BLOB_BUFFER_SIZE, bf, tmpByteBuffer, outChannel, ConfigInfo.FILE_CHARACTERSET);
							}
    	        			
							StringBuffer lobBf = new StringBuffer();
							lobBf.append("\\\\x");
							bb.put(lobBf.toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
			        		bb.flip();
			        		outChannel.write(bb);
			        		bb.clear();
			        		lobBf.setLength(0);
							while((len = in.read(buffer))!= -1) {
								buffeOutr.write(buffer, 0, len);
								buffeOutr.flush();
								lobBf.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
								divideProcessing(ConfigInfo.BLOB_BUFFER_SIZE, lobBf, bb, outChannel, ConfigInfo.FILE_CHARACTERSET);
			        			buffeOutr.reset();
							}
						}
						buffeOutr.close();	
					}
					in.close();
				
					return "";	
				}
			case Types.VARBINARY:
				byte[] bytes = rs.getBytes(index);
				return bytes == null ? "\\N" : bytes.toString();
			case Types.LONGVARBINARY:
				InputStream in = rs.getBinaryStream(index);
				if(in == null) {
					return "\\N";
				} else {
					byte[] buffer = new byte[ConfigInfo.BLOB_BUFFER_SIZE];
					int len = 0;
					int buffSize = ConfigInfo.BLOB_BUFFER_SIZE * 4+1;
					ByteBuffer bb = ByteBuffer.allocateDirect(buffSize>bf.length()?buffSize:bf.length());
					
					ByteArrayOutputStream buffeOutr = new ByteArrayOutputStream();
					StringBuffer lobBf = new StringBuffer();
					if(bf.length()>0) {
						ByteBuffer tmpByteBuffer = ByteBuffer.allocateDirect(ConfigInfo.BASIC_BUFFER_SIZE*2);
						divideProcessing(ConfigInfo.BLOB_BUFFER_SIZE, bf, tmpByteBuffer, outChannel, ConfigInfo.FILE_CHARACTERSET);
					}
        								
					
					lobBf.append("\\\\x");
					bb.put(lobBf.toString().getBytes(ConfigInfo.FILE_CHARACTERSET));
					bb.flip();
					outChannel.write(bb);
					bb.clear();
					lobBf.setLength(0);
					while((len = in.read(buffer))!= -1) {														
						buffeOutr.write(buffer, 0, len);
						buffeOutr.flush();
						lobBf.append(DatatypeConverter.printHexBinary(buffeOutr.toByteArray()));
						
						divideProcessing(ConfigInfo.BLOB_BUFFER_SIZE, lobBf, bb, outChannel, ConfigInfo.FILE_CHARACTERSET);
						
						buffeOutr.reset();
						lobBf.setLength(0);
					}
					
					
					buffeOutr.close();	
					in.close();
					return "";
				}
				
			case OracleTypes.OPAQUE: case Types.SQLXML:
				SQLXML xml = rs.getSQLXML(index); 
				return xml == null ? "\\N" : xml.toString();
			case Types.STRUCT: 
				Object obj = rs.getObject(index);
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
				NClob nclob = rs.getNClob(index);
				
				if (nclob != null) {
					BufferedReader reader = null;
					reader = new BufferedReader(nclob.getCharacterStream());
					char[] buffer = new char[ConfigInfo.CLOB_BUFFER_SIZE];
					int n = 0;
					
					if (ConfigInfo.BASIC_BUFFER_SIZE > nclob.length() + bf.length()) { 
						StringBuffer sb = new StringBuffer();
						while((n = reader.read(buffer)) != -1){
							sb.append(buffer, 0, n);		
						}
						reader.close();
						return DevUtils.replaceEach(sb.toString(), DevUtils.BackSlashSequence, DevUtils.BackSlashSequenceReplace);
					} else {
						if(bf.length()>0) {
							ByteBuffer tmpByteBuffer = ByteBuffer.allocateDirect(ConfigInfo.BASIC_BUFFER_SIZE*2);
							divideProcessing(ConfigInfo.BASIC_BUFFER_SIZE, bf, tmpByteBuffer, outChannel, ConfigInfo.FILE_CHARACTERSET);
						}
						
						ByteBuffer bb = ByteBuffer.allocateDirect(buffer.length*4);
						
						while((n = reader.read(buffer)) != -1){
							String s = new String(Arrays.copyOfRange(buffer, 0, n));
							bb.put(s.getBytes(ConfigInfo.FILE_CHARACTERSET));
							bb.flip();
			        		outChannel.write(bb);
			        		bb.clear();
						}
						reader.close();
						return "";
					}
				}
				return "\\N";
			case Types.NULL:
				return "\\N";
			default : // Other Types
				Object otherObj = rs.getObject(index);
				return otherObj == null ? "\\N" : otherObj.toString();
			}
		} catch(Exception e){
			throw e;
		}
	}
	
	protected void divideProcessing (int bufferSize, StringBuffer bf, ByteBuffer bb, GatheringByteChannel outChannel, String charset) throws IOException {
		int bfsCnt = bf.length()/bufferSize+1;
		for(int i=0;i<bfsCnt;i++) {
			String sub = null;
			if(i<bfsCnt-1) {
				sub = bf.substring(i*bufferSize, (i+1)*bufferSize);
			} else {
				sub = bf.substring(i*bufferSize, i*bufferSize+(bf.length()%bufferSize));
			}
			if(sub != null){
				bb.put(sub.getBytes(charset));
				bb.flip();
    			
    			outChannel.write(bb);
    			bb.clear();
			}
		}
		bf.setLength(0);
	}
	protected void divideProcessing (int compareSize, int size, int bufferSize, StringBuffer bf, ByteBuffer bb, GatheringByteChannel outChannel, String charset) throws IOException {
		if (compareSize >= size ){
			divideProcessing(bufferSize, bf, bb, outChannel, charset);
		}
	}
	
	protected void CloseConn(Connection conn, PreparedStatement pStmt) {
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
	
	protected void CloseConn(Connection conn, Statement stmt) {
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
