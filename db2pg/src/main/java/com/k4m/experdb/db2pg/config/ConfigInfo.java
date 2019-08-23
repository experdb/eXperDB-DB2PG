package com.k4m.experdb.db2pg.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;


public class ConfigInfo {
	public static boolean SRC_INCLUDE_DATA_EXPORT;
	public static boolean SRC_DDL_EXPORT;
	public static boolean TAR_CONSTRAINT_DDL;
			;
	
	//region SRC
	public static DBConfigInfo SRC_DB_CONFIG = new DBConfigInfo();
	//region PROCESSING
	public static int SRC_LOB_BUFFER_SIZE // 1024
			, SRC_STATEMENT_FETCH_SIZE // 3000
			;
	public static int SRC_SELECT_ON_PARALLEL // 1
			, SRC_COPY_SEGMENT_SIZE // one time copy command load tuple count (10000)
			;
	public static boolean VERBOSE // check process log (true)
			, SRC_IS_ASCII // only use source encoding is ascii (false)
			;
	//endregion
	//region OPTIONAL_PROCESSING
	public static String SRC_WHERE_CONDITION=null
			;
	public static boolean SRC_TABLE_DDL // true
			, TAR_TRUNCATE // false
			;
	public static List<String> SRC_INCLUDE_TABLES = null
			, SRC_EXCLUDE_TABLES = null
			;
	public static int SRC_ROWS_EXPORT //select rownum (-1)
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
	public static String SRC_FILE_OUTPUT_PATH // "./"
			, SRC_CLASSIFY_STRING //  original(default), toupper, tolower 
			;
	//endregion
	
	//region INPUT
	public static String SRC_FILE_QUERY_DIR_PATH = null;
	//endregion
	
	//region OUTPUT
	//buffer size
	public static int SRC_BUFFER_SIZE; //10*1024*1024
	//endregion
	
	public static org.apache.log4j.Level LOG_LEVEL;
	
	public static boolean FILE_WRITER_MODE, DB_WRITER_MODE;
	
	public static boolean TAR_FILE_APPEND;
	
	public static boolean TAR_TABLE_BAD;
	
	public static int TAR_LIMIT_ERROR;
	
	public static boolean TAR_CONSTRAINT_REBUILD;
	
	public static boolean SRC_DDL_EXT;
	
	
	public static class Loader {
		public static void load(String configFilePath) {
			try {
				Properties prop = new Properties();
				prop.load(new FileInputStream(configFilePath));
				ConfigInfo.SRC_INCLUDE_DATA_EXPORT = (boolean)propertyCheck(trimCheck(prop.getProperty("SRC_INCLUDE_DATA_EXPORT")),false,Boolean.class);
				ConfigInfo.SRC_DDL_EXPORT = (boolean)propertyCheck(trimCheck(prop.getProperty("SRC_DDL_EXPORT")),false,Boolean.class);
				ConfigInfo.TAR_CONSTRAINT_DDL = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_CONSTRAINT_DDL")),false,Boolean.class);
				SRC_DB_CONFIG.SERVERIP 		= trimCheck(prop.getProperty("SRC_HOST"));
				SRC_DB_CONFIG.USERID 		= trimCheck(prop.getProperty("SRC_USER"));
				SRC_DB_CONFIG.DB_PW 		= trimCheck(prop.getProperty("SRC_PASSWORD")); 
				SRC_DB_CONFIG.DBNAME 		= trimCheck(prop.getProperty("SRC_DATABASE")); 
				SRC_DB_CONFIG.SCHEMA_NAME 	= trimCheck(prop.getProperty("SRC_SCHEMA")); 
				SRC_DB_CONFIG.DB_TYPE 		= (String)propertyCheck(trimCheck(prop.getProperty("SRC_DBMS_TYPE")),"ORA",String.class);
				SRC_DB_CONFIG.PORT			= (String)propertyCheck(trimCheck(prop.getProperty("SRC_PORT")),"1521",String.class);
				SRC_DB_CONFIG.CHARSET 		= (String)propertyCheck(trimCheck(prop.getProperty("SRC_DB_CHARSET")),null,String.class);
				// DDL 추출을 지원하는 DBMS
				if(SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MYS) || SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.ORA) || SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MSS) || SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.TBR)){
					ConfigInfo.SRC_DDL_EXT = true;
				}else {
					ConfigInfo.SRC_DDL_EXT = false;
				}

				ConfigInfo.SRC_LOB_BUFFER_SIZE = (int)propertyCheck(trimCheck(prop.getProperty("SRC_LOB_BUFFER_SIZE")),100,Integer.class);
				ConfigInfo.SRC_LOB_BUFFER_SIZE = ConfigInfo.SRC_LOB_BUFFER_SIZE>0?ConfigInfo.SRC_LOB_BUFFER_SIZE:100;
				ConfigInfo.SRC_LOB_BUFFER_SIZE = ConfigInfo.SRC_LOB_BUFFER_SIZE * 1024 * 1024;
				
				ConfigInfo.SRC_STATEMENT_FETCH_SIZE = (int)propertyCheck(trimCheck(prop.getProperty("SRC_STATEMENT_FETCH_SIZE")),3000,Integer.class);
				ConfigInfo.SRC_SELECT_ON_PARALLEL = (int)propertyCheck(trimCheck(prop.getProperty("SRC_SELECT_ON_PARALLEL")),1,Integer.class);
				ConfigInfo.SRC_COPY_SEGMENT_SIZE = (int)propertyCheck(trimCheck(prop.getProperty("SRC_COPY_SEGMENT_SIZE")),10000,Integer.class);
				ConfigInfo.VERBOSE = (boolean)propertyCheck(trimCheck(prop.getProperty("VERBOSE")),true,Boolean.class);
				ConfigInfo.TAR_TABLE_BAD = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_TABLE_BAD")),true,Boolean.class);
				ConfigInfo.SRC_WHERE_CONDITION = trimCheck(prop.getProperty("SRC_WHERE_CONDITION"));
				ConfigInfo.SRC_TABLE_DDL = (boolean)propertyCheck(trimCheck(prop.getProperty("SRC_TABLE_DDL")),true,Boolean.class);
				ConfigInfo.TAR_TRUNCATE = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_TRUNCATE")),false,Boolean.class);
				String allowTableStrs = trimCheck(prop.getProperty("SRC_INCLUDE_TABLES"));
				if(allowTableStrs != null && !allowTableStrs.equals("")){
					List<String> tmps = new ArrayList<String>();
					for(String allowTableStr : allowTableStrs.split(",")){
						String tmp = allowTableStr.trim();
						if(tmp.equals("")) continue;
						tmps.add(tmp);
					}
					ConfigInfo.SRC_INCLUDE_TABLES = tmps.size()>0 ? tmps : null;
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
				ConfigInfo.SRC_ROWS_EXPORT = (int)propertyCheck(trimCheck(prop.getProperty("SRC_ROWS_EXPORT")),-1,Integer.class);
				TAR_DB_CONFIG.SERVERIP = trimCheck(prop.getProperty("TAR_HOST"));
				TAR_DB_CONFIG.USERID = trimCheck(prop.getProperty("TAR_USER"));
				TAR_DB_CONFIG.DB_PW = trimCheck(prop.getProperty("TAR_PASSWORD"));
				TAR_DB_CONFIG.DBNAME = trimCheck(prop.getProperty("TAR_DATABASE"));
				TAR_DB_CONFIG.SCHEMA_NAME = trimCheck(prop.getProperty("TAR_SCHEMA"));
				TAR_DB_CONFIG.DB_TYPE = trimCheck(prop.getProperty("TAR_DB_TYPE"));
				TAR_DB_CONFIG.PORT = (String)propertyCheck(trimCheck(prop.getProperty("TAR_PORT")),"5432",String.class);
				TAR_DB_CONFIG.CHARSET = (String)propertyCheck(trimCheck(prop.getProperty("TAR_DB_CHARSET")),null,String.class);
				String outputDirectory = ((String)propertyCheck(trimCheck(prop.getProperty("SRC_FILE_OUTPUT_PATH")),"./",String.class)).trim().replace("\\", "/");
				ConfigInfo.SRC_FILE_OUTPUT_PATH = outputDirectory.length()-1 == outputDirectory.lastIndexOf("/")
													? outputDirectory : outputDirectory.concat("/");
				ConfigInfo.SRC_CLASSIFY_STRING = (String)propertyCheck(trimCheck(prop.getProperty("SRC_CLASSIFY_STRING")),"original",String.class);
				ConfigInfo.SRC_FILE_QUERY_DIR_PATH = (String)propertyCheck(trimCheck(prop.getProperty("SRC_FILE_QUERY_DIR_PATH")),"",String.class);
				ConfigInfo.SRC_BUFFER_SIZE=(int)propertyCheck(trimCheck(prop.getProperty("SRC_BUFFER_SIZE")),10,Integer.class);
				ConfigInfo.SRC_BUFFER_SIZE = ConfigInfo.SRC_BUFFER_SIZE>0?ConfigInfo.SRC_BUFFER_SIZE:10;
				ConfigInfo.SRC_BUFFER_SIZE = ConfigInfo.SRC_BUFFER_SIZE * 1024 * 1024;
				
				ConfigInfo.LOG_LEVEL = (org.apache.log4j.Level)propertyCheck(trimCheck(prop.getProperty("LOG_LEVEL"))
						,org.apache.log4j.Level.INFO,org.apache.log4j.Level.class);
				ConfigInfo.SRC_IS_ASCII = (boolean)propertyCheck(trimCheck(prop.getProperty("SRC_IS_ASCII")),false,Boolean.class);
				ConfigInfo.TAR_CONN_COUNT = (int)propertyCheck(trimCheck(prop.getProperty("TAR_CONN_COUNT")),1,Integer.class);
				ConfigInfo.TAR_TABLE_BAD_COUNT = (int)propertyCheck(trimCheck(prop.getProperty("TAR_TABLE_BAD_COUNT")),-1,Integer.class);
				ConfigInfo.TAR_COPY_OPTIONS = (String)propertyCheck(trimCheck(prop.getProperty("TAR_COPY_OPTIONS")),null,String.class);
				
				ConfigInfo.TAR_LIMIT_ERROR = (int)propertyCheck(trimCheck(prop.getProperty("TAR_LIMIT_ERROR")),0,Integer.class);
				
				ConfigInfo.FILE_WRITER_MODE = (boolean)propertyCheck(trimCheck(prop.getProperty("FILE_WRITER_MODE")),false,Boolean.class);
				ConfigInfo.TAR_FILE_APPEND = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_FILE_APPEND")),false,Boolean.class);
				ConfigInfo.DB_WRITER_MODE = (boolean)propertyCheck(trimCheck(prop.getProperty("DB_WRITER_MODE")),false,Boolean.class);
				
				ConfigInfo.TAR_CONSTRAINT_REBUILD = (boolean)propertyCheck(trimCheck(prop.getProperty("TAR_CONSTRAINT_REBUILD")),true,Boolean.class);
				
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
