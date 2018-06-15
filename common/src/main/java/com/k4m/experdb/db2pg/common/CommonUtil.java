package com.k4m.experdb.db2pg.common;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.k4m.experdb.db2pg.db.datastructure.DataTable;

public class CommonUtil {
	
	/*
	public static String GetDataFromXml(String FilePath, String FuncName, String DbType, String DbVer, String Attr) throws SAXException, IOException, ParserConfigurationException, XPathExpressionException{
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		
		InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(FilePath);
		
		Document doc = builder.parse(input);
		
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        
		String Path = String.format("/root/function[@id='%s']/%s[@min_ver<='%s' and @max_ver>='%s']/%s", FuncName, DbType, DbVer,DbVer,Attr);
        Element list = (Element) xpath.evaluate(Path, doc, XPathConstants.NODE);
        
        if (list == null || !list.hasChildNodes()){
        	return null;
        }
        
		Node node = list.getLastChild();

		String sql = node.getNodeValue();
		return sql;
	}
	*/
	/*
	public static String GetDataFromXml(String FilePath, String FuncName, String DbType, String DbVer, String Attr) throws SAXException, IOException, ParserConfigurationException, XPathExpressionException{
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		
		Document doc = builder.parse(new File(FilePath));
		
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        
		String Path = String.format("/root/function[@id='%s']/%s[@min_ver<='%s' and @max_ver>='%s']/%s", FuncName, DbType, DbVer,DbVer,Attr);
		//String Path = String.format("/root/function[@id='%s']/%s[@min_ver<='%s']/%s", FuncName, DbType, DbVer,Attr);
        Element list = (Element) xpath.evaluate(Path, doc, XPathConstants.NODE);
        
        if (list == null || !list.hasChildNodes()){
        	return null;
        }
        
		Node node = list.getLastChild();

		String sql = node.getNodeValue();
		return sql;
	}
	*/
	public static String getProcessID(){
		String name = ManagementFactory.getRuntimeMXBean().getName(); 
		System.out.println(name);
		String pidNumber = name.substring(0, name.indexOf("@"));	 // PID 번호 : 윈도/유닉스 공통
		return pidNumber;
	}
	
	public static String getStackTrace(final Throwable throwable) {
		if(throwable == null) return null;
	     final StringWriter sw = new StringWriter();
	     final PrintWriter pw = new PrintWriter(sw, true);
	     
	     throwable.printStackTrace(pw);
	     String msg = sw.getBuffer().toString();
	     
	     if (msg == null){
	    	 return throwable.getMessage();
	     }else{
		     return sw.getBuffer().toString();
	     }     
	}
	
	public static int BigDecimalToInt(Object value) {
		 if (value instanceof BigDecimal){
			 return ((BigDecimal)value).intValue();
		 }else{
			 return (Integer)value;
		 }	     
	}
	
	public static long BigDecimalToLong(Object value) {
		 if (value instanceof BigDecimal){
			 return ((BigDecimal)value).longValue();
		 }else{
			 return (Long)value;
		 }	     
	}
	
	public  static byte[] intToByteArray(int value) {
		byte[] byteArray = new byte[4];
		byteArray[0] = (byte)(value >> 24);
		byteArray[1] = (byte)(value >> 16);
		byteArray[2] = (byte)(value >> 8);
		byteArray[3] = (byte)(value);
		return byteArray;
	}
	
	public static void printResult(DataTable dt){
		try{
			for(String columnNm : dt.getColumns()){
				System.out.print(StringUtils.rightPad(columnNm, 40, " "));
			}
			LogUtils.info(StringUtils.leftPad("", dt.getColumns().size() * 40, "="),CommonUtil.class);
			
			for(Map<String, Object> map: dt.getRows()){
				for(String columnNm : dt.getColumns()){
					LogUtils.info(StringUtils.rightPad(String.valueOf(map.get(columnNm)), 40, " "),CommonUtil.class);
				}
				LogUtils.info(Constant.R,CommonUtil.class);
			}
		}catch(Exception e){
			
		}

	}
}
