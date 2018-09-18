package com.k4m.experdb.db2pg.common;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.FastDateFormat;

public class StrUtil { 
	public static final Pattern PSQL_TIMING_PATTERN = Pattern.compile("^.*[:].*[ms]$");
	
	public static String psqlTimeStrToElapsedTimeStr(String value) {
		return makeElapsedTimeString(psqlTimeStrToLong(value));
	}
	
	public static long psqlTimeStrToLong(String value) {
		int start, end;
		if(PSQL_TIMING_PATTERN.matcher(value).matches()) {
			if((start = value.indexOf(":")) != -1  && (end = value.indexOf("ms")) != -1){
				end = value.lastIndexOf(".");
				return Long.valueOf(value.substring(start+1,end).trim());
			}
		}
		return -1;
	}
	
	public static String makeElapsedTimeString(long elapsedTime) {
		StringBuilder sb = new StringBuilder();
		if(elapsedTime>=60*60) {
			int hour = (int)(elapsedTime/(60*60));
			sb.append(hour);
			sb.append("h ");
			elapsedTime = elapsedTime - hour * 60 * 60;
		} 
		if(elapsedTime>=60) {
			int min = (int)(elapsedTime/60);
			sb.append(min);
			sb.append("m ");
			elapsedTime = elapsedTime - min * 60;
		} 

		sb.append(elapsedTime);
		sb.append("s");
		return sb.toString();
	}
	
	public static boolean checkRegexPattern(String data, Pattern pattern) {
		
		if (pattern.matcher(data).matches())
			return true;
        
		return false;
	}
	
	public static boolean checkRegexPattern(String data, String patternString) {
		
		Pattern pattern = Pattern.compile(patternString);
		
		if (pattern.matcher(data).matches())
			return true;
        
		return false;
	}
	
	public static String getCalFormat(Calendar cal, String format){
		String str= "";
		try{
			str = FastDateFormat.getInstance(format).format(cal);
		}catch( Exception ex ){
			ex.printStackTrace();
		}
        
        return str;
	}
	
	public static String getCurTime(String format){
		String str= "";
		try{
			Calendar cal = Calendar.getInstance();	
			str = FastDateFormat.getInstance(format).format(cal);
		}catch( Exception ex ){
			ex.printStackTrace();
		}
        
        return str;
	}
	
	public static String getCurTime(){		
		Timestamp ts = null;
		
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String d = sdf.format(new java.util.Date());
			ts = Timestamp.valueOf(d);
		}catch( Exception ex ){
			ex.printStackTrace();
		}
        
        return ts.toString().substring( 11, 19 );
	}
	
	public static String getCurAllTime(){
		Timestamp ts = null;
		
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String d = sdf.format(new java.util.Date());
			ts = Timestamp.valueOf(d);
		}catch( Exception ex ){
			ex.printStackTrace();
		}
        
        return ts.toString().substring( 0, 19 );
	}
	
	public static String getMonthName(){
		
		String[] month = { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December" };
		String mon = null;
		
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String d = sdf.format(new java.util.Date());
			Timestamp ts = Timestamp.valueOf(d);
			mon = ts.toString().substring( 5, 7 );
			
			if( mon.startsWith( "0" ) ){
				mon = mon.substring( 1, 2 );
			}
			
		}catch( Exception ex ){
			ex.printStackTrace();
		}
		
        return month[ Integer.parseInt( mon )-1 ];
	}
	
	public static String getYear(){		
		Timestamp ts = null;
		
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String d = sdf.format(new java.util.Date());
			ts = Timestamp.valueOf(d);
		}catch( Exception ex ){
			ex.printStackTrace();
		}
        
        return ts.toString().substring( 0, 4 );
	}
	
	public static String getDate(){		
		Timestamp ts = null;
		
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String d = sdf.format(new java.util.Date());
			ts = Timestamp.valueOf(d);
		}catch( Exception ex ){
			ex.printStackTrace();
		}
        
        return ts.toString().substring( 8, 10 );
	}
	
	public static String getCurTime( int startIdx, int endIdx ){		
		Timestamp ts = null;
		
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String d = sdf.format(new java.util.Date());
			ts = Timestamp.valueOf(d);
		}catch( Exception ex ){
			ex.printStackTrace();
		}
        
        return ts.toString().substring( startIdx, endIdx );
	}
	
	public static String getNameStartsWithUpperCase( String sName, String sDelim ){
		
		String sNewName = new String();
		StringTokenizer tokenizer = new StringTokenizer( sName, sDelim );
		String str = "";
		boolean bFirstToken = true;
		while( tokenizer.hasMoreTokens() ){			
			if( bFirstToken ){
				bFirstToken = false;
				tokenizer.nextElement();
				continue;
			} 
			str = (String)tokenizer.nextElement();
			str = str.substring( 0, 1 ).toUpperCase() + str.substring( 1 );			
			sNewName += str;
		}
		return sNewName;
	}
	
	public static String nvl( String str ){
		if( str == null ){
			return "";
		}
		
		return str.trim();
	}
	
	public static boolean isNvl( String str ){
		if( str == null || str.trim().length() == 0 ){
			return true;
		}
		
		return false;
	}
	
	public static String strGetLine(String strErrMsg) throws Exception {
		String strLine = "0";
		
		int idx = strErrMsg.indexOf("line");
		if(idx > -1) {
			strLine = strErrMsg.substring(idx+5);
		}
		
		return strLine;
	}
}
