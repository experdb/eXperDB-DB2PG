package com.k4m.experdb.db2pg.common;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;

public class CommonUtil {
	
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
	
}
