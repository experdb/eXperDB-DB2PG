package com.k4m.experdb.db2pg.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseUtil {
	public static String ConvertClobToString(StringBuffer sb, Clob clob) throws SQLException, IOException{
		Reader reader = clob.getCharacterStream();
		char[] buffer = new char[(int)clob.length()];
		while(reader.read(buffer) != -1){
			sb.append(buffer);
		}
		return sb.toString();
	}
	
	public static Object PreparedStmtSetValue(int columnType, ResultSet rs, int index) throws SQLException, IOException{
		StringBuffer sb = new StringBuffer();
		switch(columnType){
		case 2005:  //CLOB
			Clob clob = rs.getClob(index);
			
			if (clob == null){
				return null;
			}
			
			Reader reader = clob.getCharacterStream();
			char[] buffer = new char[(int)clob.length()];
			while(reader.read(buffer) != -1){
				sb.append(buffer);				
			}
			return sb.toString();
		case 2004:  //BLOB			
			Blob blob = rs.getBlob(index);
			
			if (blob == null){
				return null;
			}
			
			InputStream in = blob.getBinaryStream();
			byte[] Bytebuffer = new byte[(int)blob.length()];
			in.read(Bytebuffer);
			return Bytebuffer;
		case -2:
			return rs.getBytes(index);
		default:
			return rs.getObject(index);
		}	
	}
	/*
	public static void PreparedStmtSetValue(int columnType, ResultSet rs, PreparedStatement preStmt, int index) throws SQLException, IOException{
		switch(columnType){
		case 2005:  //CLOB
			preStmt.setClob(index, rs.getClob(index));	
			break;
		case 2004:	//BLOB		
			preStmt.setBlob(index, rs.getBlob(index));
			break;			
			
		case -3 : //VARBINARY
			preStmt.setBytes(index, rs.getBytes(index));
			break;
			
		default:
			preStmt.setObject(index, rs.getObject(index));
			break;
		}	
	}
	*/
	public static String QueryIncreateInParam(String query, int count){
		if (count < 1) return query;
		StringBuilder builder = new StringBuilder();
		builder.append("IN(");
		for(int i=0; i<count; i++){
			if (i == count -1){
				builder.append("?");
			}else{
				builder.append("?,");
			}
		}
		builder.append(")");
		query = query.replace("IN(?)", builder.toString());
		return query;
	}
}
