package com.k4m.experdb.db2pg.sample;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.imageio.ImageIO;

import com.k4m.experdb.db2pg.config.ArgsParser;

public class ByteAImgCheck {

	public static void main(String[] args) throws Exception {
		
		ArgsParser argsParser = new ArgsParser();
		argsParser.parse(args);
		
		String url = "jdbc:postgresql://222.110.153.162:5432/ibizspt";
	    String user = "ibizspt";
	    String password = "ibizspt";

	    String query = "SELECT image_name, blob_content FROM ibizspt.wwv_flow_random_images LIMIT 1";
	    
	    Connection con = DriverManager.getConnection(url, user, password);
	    PreparedStatement pst = con.prepareStatement(query);
	    ResultSet rs = pst.executeQuery();
	    
	    try {
		    while (rs.next()) {
	            
		    	String strImgName = rs.getString(1);
		    	byte[] byteImg = rs.getBytes(2);
		    	
		    	byteArrayConvertToImageFile(byteImg, strImgName, "C:\\k4m\\01-1. DX 제폼개발\\06. DX-Tcontrol\\07. 시험\\", "gif");
	        }
	    } catch(Exception e) {
	    	
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
