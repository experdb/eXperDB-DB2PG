package com.k4m.experdb.db2pg.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;


public class ConfigInfo {
	public static boolean SRC_EXPORT;
	public static boolean SRC_DDL_EXPORT;
	public static boolean TAR_CONSTRAINT_EXTRACT;
			;
	
	//region SRC
	public static DBConfigInfo SRC_DB_CONFIG = new DBConfigInfo();
	//region PROCESSING
	public static int SRC_LOB_FETCH_SIZE // 1024
			, STATEMENT_FETCH_SIZE // 3000
			;
	public static int SRC_TABLE_SELECT_PARALLEL // 1
			, SRC_TABLE_COPY_SEGMENT_SIZE // one time copy command load tuple count (10000)
			;
	public static boolean VERBOSE // check process log (true)
			, SRC_IS_ASCII // only use source encoding is ascii (false)
			;
	//endregion
	//region OPTIONAL_PROCESSING
	public static String SRC_WHERE=null
			;
	public static boolean TABLE_ONLY // true
			, TRUNCATE // false
			;
	public static List<String> SRC_ALLOW_TABLES = null
			, SRC_EXCLUDE_TABLES = null
			;
	public static int SRC_ROWNUM //select rownum (-1)
			;
	//endregion
	//endregion
	
	//region TAR
	public static DBConfigInfo TAR_DB_CONFIG = new DBConfigInfo();
	public static int TAR_CONN_COUNT, TAR_TABLE_BAD_COUNT
			;
	public static String TAR_COPY_OPTIONS
			;
	
	//endregion
	
	//region OUTPUT
	public static String OUTPUT_DIRECTORY // "./"
			, CLASSIFY_STRING //  original(default), small, capital 
			;
	//endregion
	
	//region INPUT
	public static String SELECT_QUERIES_FILE = null;
	//endregion
	
	//region OUTPUT
	//buffer size
	public static int BUFFER_SIZE; //10*1024*1024
	//endregion
	
	public static org.apache.log4j.Level LOG_LEVEL;
	
	public static boolean FILE_WRITER_MODE, DB_WRITER_MODE;
	
	public static boolean TAR_TABLE_BAD;
	
	public static int TAR_TABLE_ERR_CNT_EXIT;
	
	public static boolean TAR_DROP_CREATE_CONSTRAINT;
	
	
	public static class Loader {
		public static void load(String configFilePath) {
			try {
				Properties prop = new Properties();
				prop.load(new FileInputStream(configFilePath));
				ConfigInfo.SRC_EXPORT = (boolean)propertyCheck(trimCheck(prop.getProperty("SRC_EXPORT")),false,Boolean.class);
				ConfigInfo.SRC_DDL_EXPORT = (boolean)propertyCheck(trimCheck(prop.getProperty("SRC_DDL_EXPORT")),false,Boolean.class);
				ConfigInfo.TAR_CONSTRAINT_EXTRACT = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_CONSTRAINT_EXTRACT")),false,Boolean.class);
				SRC_DB_CONFIG.SERVERIP 		= trimCheck(prop.getProperty("SRC_HOST"));
				SRC_DB_CONFIG.USERID 		= trimCheck(prop.getProperty("SRC_USER"));
				SRC_DB_CONFIG.DB_PW 		= trimCheck(prop.getProperty("SRC_PASSWORD")); 
				SRC_DB_CONFIG.DBNAME 		= trimCheck(prop.getProperty("SRC_DATABASE")); 
				SRC_DB_CONFIG.SCHEMA_NAME 	= trimCheck(prop.getProperty("SRC_SCHEMA")); 
				SRC_DB_CONFIG.DB_TYPE 		= (String)propertyCheck(trimCheck(prop.getProperty("SRC_DB_TYPE")),"ORA",String.class);
				SRC_DB_CONFIG.PORT			= (String)propertyCheck(trimCheck(prop.getProperty("SRC_PORT")),"1521",String.class);
				SRC_DB_CONFIG.CHARSET 		= (String)propertyCheck(trimCheck(prop.getProperty("SRC_DB_CHARSET")),null,String.class);
				ConfigInfo.SRC_LOB_FETCH_SIZE = (int)propertyCheck(trimCheck(prop.getProperty("SRC_LOB_FETCH_SIZE")),1024,Integer.class);
				ConfigInfo.STATEMENT_FETCH_SIZE = (int)propertyCheck(trimCheck(prop.getProperty("STATEMENT_FETCH_SIZE")),3000,Integer.class);
				ConfigInfo.SRC_TABLE_SELECT_PARALLEL = (int)propertyCheck(trimCheck(prop.getProperty("SRC_TABLE_SELECT_PARALLEL")),1,Integer.class);
				ConfigInfo.SRC_TABLE_COPY_SEGMENT_SIZE = (int)propertyCheck(trimCheck(prop.getProperty("SRC_TABLE_COPY_SEGMENT_SIZE")),10000,Integer.class);
				ConfigInfo.VERBOSE = (boolean)propertyCheck(trimCheck(prop.getProperty("VERBOSE")),true,Boolean.class);
				ConfigInfo.TAR_TABLE_BAD = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_TABLE_BAD")),true,Boolean.class);
				ConfigInfo.SRC_WHERE = trimCheck(prop.getProperty("SRC_WHERE"));
				ConfigInfo.TABLE_ONLY = (boolean)propertyCheck(trimCheck(prop.getProperty("TABLE_ONLY")),true,Boolean.class);
				ConfigInfo.TRUNCATE = (boolean)propertyCheck(trimCheck(prop.getProperty("TRUNCATE")),true,Boolean.class);
				String allowTableStrs = trimCheck(prop.getProperty("SRC_ALLOW_TABLES"));
				if(allowTableStrs != null && !allowTableStrs.equals("")){
					List<String> tmps = new ArrayList<String>();
					for(String allowTableStr : allowTableStrs.split(",")){
						String tmp = allowTableStr.trim();
						if(tmp.equals("")) continue;
						tmps.add(tmp);
					}
					ConfigInfo.SRC_ALLOW_TABLES = tmps.size()>0 ? tmps : null;
				}
				String excludeTableStrs = trimCheck(prop.getProperty("SRC_EXCLUDE_TABLES"));
				if(excludeTableStrs != null && !excludeTableStrs.equals("")){
					List<String> tmps = new ArrayList<String>();
					for(String excludeTableStr : excludeTableStrs.split(",")){
						String tmp = excludeTableStr.trim();
						if(tmp.equals("")) continue;
						tmps.add(tmp);
					}
					ConfigInfo.SRC_EXCLUDE_TABLES = tmps.size()>0 ? tmps : null;
				}
				ConfigInfo.SRC_ROWNUM = (int)propertyCheck(trimCheck(prop.getProperty("SRC_ROWNUM")),-1,Integer.class);
				TAR_DB_CONFIG.SERVERIP = trimCheck(prop.getProperty("TAR_HOST"));
				TAR_DB_CONFIG.USERID = trimCheck(prop.getProperty("TAR_USER"));
				TAR_DB_CONFIG.DB_PW = trimCheck(prop.getProperty("TAR_PASSWORD"));
				TAR_DB_CONFIG.DBNAME = trimCheck(prop.getProperty("TAR_DATABASE"));
				TAR_DB_CONFIG.SCHEMA_NAME = trimCheck(prop.getProperty("TAR_SCHEMA"));
				TAR_DB_CONFIG.DB_TYPE = trimCheck(prop.getProperty("TAR_DB_TYPE"));
				TAR_DB_CONFIG.PORT = (String)propertyCheck(trimCheck(prop.getProperty("TAR_PORT")),"5432",String.class);
				TAR_DB_CONFIG.CHARSET = (String)propertyCheck(trimCheck(prop.getProperty("TAR_DB_CHARSET")),null,String.class);
				String outputDirectory = ((String)propertyCheck(trimCheck(prop.getProperty("OUTPUT_DIRECTORY")),"./",String.class)).trim().replace("\\", "/");
				ConfigInfo.OUTPUT_DIRECTORY = outputDirectory.length()-1 == outputDirectory.lastIndexOf("/")
													? outputDirectory : outputDirectory.concat("/");
				ConfigInfo.CLASSIFY_STRING = (String)propertyCheck(trimCheck(prop.getProperty("CLASSIFY_STRING")),"original",String.class);
				ConfigInfo.SELECT_QUERIES_FILE = (String)propertyCheck(trimCheck(prop.getProperty("SELECT_QUERIES_FILE")),"",String.class);
				ConfigInfo.BUFFER_SIZE=(int)propertyCheck(trimCheck(prop.getProperty("BUFFER_SIZE")),10,Integer.class);
				ConfigInfo.BUFFER_SIZE = ConfigInfo.BUFFER_SIZE>0?ConfigInfo.BUFFER_SIZE:10;
				ConfigInfo.BUFFER_SIZE = ConfigInfo.BUFFER_SIZE * 1024 * 1024;
				
				ConfigInfo.LOG_LEVEL = (org.apache.log4j.Level)propertyCheck(trimCheck(prop.getProperty("LOG_LEVEL"))
						,org.apache.log4j.Level.INFO,org.apache.log4j.Level.class);
				ConfigInfo.SRC_IS_ASCII = (boolean)propertyCheck(trimCheck(prop.getProperty("SRC_IS_ASCII")),false,Boolean.class);
				ConfigInfo.TAR_CONN_COUNT = (int)propertyCheck(trimCheck(prop.getProperty("TAR_CONN_COUNT")),1,Integer.class);
				ConfigInfo.TAR_TABLE_BAD_COUNT = (int)propertyCheck(trimCheck(prop.getProperty("TAR_TABLE_BAD_COUNT")),-1,Integer.class);
				ConfigInfo.TAR_COPY_OPTIONS = (String)propertyCheck(trimCheck(prop.getProperty("TAR_COPY_OPTIONS")),null,String.class);
				
				ConfigInfo.TAR_TABLE_ERR_CNT_EXIT = (int)propertyCheck(trimCheck(prop.getProperty("TAR_TABLE_ERR_CNT_EXIT")),0,Integer.class);
				
				ConfigInfo.FILE_WRITER_MODE = (boolean)propertyCheck(trimCheck(prop.getProperty("FILE_WRITER_MODE")),false,Boolean.class);
				ConfigInfo.DB_WRITER_MODE = (boolean)propertyCheck(trimCheck(prop.getProperty("DB_WRITER_MODE")),false,Boolean.class);
				
				ConfigInfo.TAR_DROP_CREATE_CONSTRAINT = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_DROP_CREATE_CONSTRAINT")),true,Boolean.class);

				
			} catch (FileNotFoundException fnfe) {
				LogUtils.error("[CONFIG_FILE_NOT_FOUND_ERR]",ConfigInfo.Loader.class,fnfe);
			} catch (IOException ioe) {
				LogUtils.error("[CONFIG_FILE_LOAD_ERR]",Loader.class,ioe);
			}
		}
		
		private static Object propertyCheck(String value,Object defValue, Class<?> clazz) {
			if(clazz == Integer.class) {
				return value != null && !value.equals("") ? Integer.valueOf(value) : defValue;
			} else if(clazz == Long.class) {
				return value != null && !value.equals("") ? Long.valueOf(value) : defValue;
			} else if(clazz == Boolean.class) {
				return value != null && !value.equals("") ? Boolean.valueOf(value) : defValue;
			} else if(clazz == Mode.class) {
				if(Mode.SRC.name().equals(value)){
					return Mode.SRC;
				} else if (Mode.TAR.name().equals(value)) {
					return Mode.TAR;
				} else {
					return Mode.ALL;
				}
			} else if(clazz == org.apache.log4j.Level.class) {
				if(value != null && !value.equals("")) {
					switch(value.toUpperCase()) {
					case "OFF":		return org.apache.log4j.Level.OFF;
					case "FATAL":	return org.apache.log4j.Level.FATAL;
					case "ERROR":	return org.apache.log4j.Level.ERROR;
					case "WARN":	return org.apache.log4j.Level.WARN;
					case "INFO":	return org.apache.log4j.Level.INFO;
					case "DEBUG":	return org.apache.log4j.Level.DEBUG;
					case "TRACE":	return org.apache.log4j.Level.TRACE;
					case "ALL":		return org.apache.log4j.Level.ALL;
					default :		return defValue;
					}
				} else return defValue;
			} else {
				return value != null && !value.equals("") ? value : defValue;
			}
		}
	}
	
	public enum Mode { SRC,TAR,ALL }
	
	private static String trimCheck(String property){
		if(property!=null){
			property = property.trim();
		}
		return property;
	}
}
