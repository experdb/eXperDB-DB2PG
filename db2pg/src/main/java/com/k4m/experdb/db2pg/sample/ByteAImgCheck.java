package com.k4m.experdb.db2pg.sample;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.imageio.ImageIO;

import com.k4m.experdb.db2pg.config.ArgsParser;

import oracle.sql.BLOB;

public class ByteAImgCheck {

	public static void main(String[] args) throws Exception {
		
		
		//oracle
		oracleImgSave(args);
		
		//postgreSql
//		postgreSqlImgSave(args);
		
		
	}
	
	private static void postgreSqlImgSave(String[] args) throws Exception {
		ArgsParser argsParser = new ArgsParser();
		argsParser.parse(args);
		
		String url = "jdbc:postgresql://ip:port/db2pg";
	    String user = "db2pg";
	    String password = "db2pg";

	    String query = "SELECT image_name, blob_content FROM db2pg.wwv_flow_random_images limit 10";
	    
	    Connection con = DriverManager.getConnection(url, user, password);
	    PreparedStatement pst = con.prepareStatement(query);
	    ResultSet rs = pst.executeQuery();
	    
	    try {
		    while (rs.next()) {
	            
		    	String strImgName = rs.getString(1);
		    	byte[] byteImg = rs.getBytes(2);
		    	
		    	int pos = strImgName.lastIndexOf( "." );
		    	String ext = strImgName.substring( pos + 1 );

		    	
		    	byteArrayConvertToImageFile(byteImg, strImgName, "C:\\Users\\1\\Desktop\\imgCheck\\target_PostgreSQL\\", ext);
	        }
	    } catch(Exception e) {
	    	
	    } finally {
	    	if(rs != null) rs.close();
	    	if(con != null) con.close();
	    }
		
	}

	private static void oracleImgSave(String[] args) throws Exception {
		ArgsParser argsParser = new ArgsParser();
		argsParser.parse(args);
		
		String url = "jdbc:oracle:thin:@ip:port/pidsvr";
	    String user = "db2pg";
	    String password = "db2pg";

	    String query = "SELECT image_name, blob_content FROM DB2PG.wwv_flow_random_images where rownum < 11";
	    
	    Connection con = DriverManager.getConnection(url, user, password);
	    PreparedStatement pst = con.prepareStatement(query);
	    ResultSet rs = pst.executeQuery();
	    
	    try {
		    while (rs.next()) {
	            
		    	String strImgName = rs.getString(1);
		    	//byte[] byteImg = rs.getBytes(2);
		    	
		    	BLOB blob = (BLOB) rs.getBlob(2);
		    	
		    	int pos = strImgName.lastIndexOf( "." );
		    	String ext = strImgName.substring( pos + 1 );
		    	
		    	
		    	InputStream in = blob.getBinaryStream();
		    	FileOutputStream out = new FileOutputStream("C:\\Users\\1\\Desktop\\imgCheck\\source_Oracle\\" + strImgName + "." + ext);
		    	
		    	int size = blob.getBufferSize();
		    	byte[] buffer = new byte[size];
		    	int length = -1;
		    	
		    	while((length = in.read(buffer)) != -1) {
		    		out.write(buffer, 0, length);
		    	}

		    	out.close();
		    	in.close();
		    	//byteArrayConvertToImageFile(byteImg, strImgName, "C:\\k4m\\01-1. DX 제폼개발\\06. DX-Tcontrol\\07. 시험\\byteaImages\\", ext);
	        }
	    } catch(Exception e) {
	    	e.printStackTrace();
	    } finally {
	    	if(rs != null) rs.close();
	    	if(con != null) con.close();
	    }
		
	}
	
	public static void byteArrayConvertToImageFile(byte[] imageByte, String strFileName, String strPath, String strExtension) throws Exception
	{
	  ByteArrayInputStream inputStream = new ByteArrayInputStream(imageByte);
	  BufferedImage bufferedImage = ImageIO.read(inputStream);

	  ImageIO.write(bufferedImage, strExtension, new File(strPath + strFileName)); 
	}
	
	


}
