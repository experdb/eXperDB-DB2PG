package com.k4m.experdb.db2pg;

import java.io.File;

import org.apache.log4j.LogManager;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ArgsParser;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.DDLConverter;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.DBUtils;
import com.k4m.experdb.db2pg.rebuild.MakeSqlFile;
import com.k4m.experdb.db2pg.rebuild.TargetPgDDL;
import com.k4m.experdb.db2pg.unload.ManagementConstraint;
import com.k4m.experdb.db2pg.unload.Unloader;
import com.k4m.experdb.db2pg.config.MsgCode;


public class Main {  
	static MsgCode msgCode = new MsgCode();

	public static void main(String[] args) throws Exception {
		msgCode.loadCode();
		
		ArgsParser argsParser = new ArgsParser();
		argsParser.parse(args);
		LogUtils.setVerbose(ConfigInfo.VERBOSE);
		LogManager.getRootLogger().setLevel(ConfigInfo.LOG_LEVEL);
		
		// 디버깅 모드 동작시 mybatis의 디버깅을 위하여 logger 추가
		if ( ConfigInfo.LOG_LEVEL == org.apache.log4j.Level.DEBUG ) {
			org.apache.log4j.ConsoleAppender appender = new org.apache.log4j.ConsoleAppender();
			appender.setName("sqlAppender");
			
			org.apache.log4j.PatternLayout layout = new org.apache.log4j.PatternLayout();
			layout.setConversionPattern("%d [%t] %-5p %c{1} - %m%n");
			appender.setLayout(layout);
			
			org.apache.log4j.varia.StringMatchFilter strMatchFilter = new org.apache.log4j.varia.StringMatchFilter();
			strMatchFilter.setStringToMatch("Result");
			strMatchFilter.setAcceptOnMatch(false);
			appender.addFilter(strMatchFilter);
			
			org.apache.log4j.Logger[] queryLogger = { 
				  LogManager.getLogger("org.mybatis")
				, LogManager.getLogger("java.sql")
			};
			for (org.apache.log4j.Logger logger : queryLogger) {
				logger.setLevel(org.apache.log4j.Level.DEBUG);
				logger.setAdditivity(false);
				logger.removeAllAppenders();
				logger.addAppender(appender);
			}
		}

		// Target DBMS's Character SET Check
		if(ConfigInfo.SRC_DDL_EXPORT) {
			if(ConfigInfo.TAR_DB_CONFIG.CHARSET == null || ConfigInfo.TAR_DB_CONFIG.CHARSET.equals("")) {
				System.exit(Constant.ERR_CD.CONFIG_NOT_FOUND);
			}
		}

		// create pool
		createPool();
		
		LogUtils.info(msgCode.getCode("M0001"),Main.class);
		
		// check output directory 
		// checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_PATH);
		makeDirectory();
		
		// Target DB Charset different vs config Charset
		String pgCharSet = DBUtils.getCharSet(Constant.POOLNAME.TARGET.name(), ConfigInfo.TAR_DB_CONFIG);
		if( (ConfigInfo.DB_WRITER_MODE || ConfigInfo.FILE_WRITER_MODE) && ConfigInfo.SRC_INCLUDE_DATA_EXPORT) {
			if(pgCharSet != null && !pgCharSet.toUpperCase().equals(ConfigInfo.TAR_DB_CONFIG.CHARSET.toUpperCase())) {
				LogUtils.error("Target Database Charset is "+pgCharSet +". Exit.", Unloader.class);
				System.exit(Constant.ERR_CD.FAIL_CHARSET);
			}
		}
		
		if(ConfigInfo.SRC_DDL_EXPORT) {
			LogUtils.debug(msgCode.getCode("M0002"),Main.class);
			DDLConverter ddlConv = DDLConverter.getInstance();
			ddlConv.start();
			LogUtils.debug(msgCode.getCode("M0003"),Main.class);
		}
		
		if(ConfigInfo.TAR_CONSTRAINT_DDL && ConfigInfo.DB_WRITER_MODE && ConfigInfo.SRC_INCLUDE_DATA_EXPORT) {
			TargetPgDDL targetPgDDL = new TargetPgDDL();
			makeSqlFile(targetPgDDL);
		}
		
		if( (ConfigInfo.SRC_INCLUDE_DATA_EXPORT || checkQueryXml()) && (ConfigInfo.DB_WRITER_MODE || ConfigInfo.FILE_WRITER_MODE)) {
			TargetPgDDL dbInform = null ;
			if(ConfigInfo.DB_WRITER_MODE ) dbInform = new TargetPgDDL();
			
			ManagementConstraint managementConstraint = new ManagementConstraint();
			
			if(ConfigInfo.DB_WRITER_MODE ) {
				LogUtils.debug(msgCode.getCode("M0004"),Main.class);
				makeSqlFile(dbInform);
				LogUtils.debug(msgCode.getCode("M0005"),Main.class);
			}

			if(ConfigInfo.DB_WRITER_MODE && ConfigInfo.TAR_CONSTRAINT_REBUILD) {
				LogUtils.debug(msgCode.getCode("M0006"),Main.class);
				managementConstraint.dropFk(dbInform);
				LogUtils.debug(msgCode.getCode("M0007"),Main.class);
				
				LogUtils.debug(msgCode.getCode("M0008"),Main.class);
				managementConstraint.dropIndex(dbInform);
				LogUtils.debug(msgCode.getCode("M0009"),Main.class);
			}

			LogUtils.debug(msgCode.getCode("M0010"),Main.class);
			Unloader loader = new Unloader();
			loader.start();	
			LogUtils.debug(msgCode.getCode("M0011"),Main.class);

			if(ConfigInfo.DB_WRITER_MODE && ConfigInfo.TAR_CONSTRAINT_REBUILD) {					
				LogUtils.debug(msgCode.getCode("M0012"),Main.class);
				managementConstraint.createIndex(dbInform);
				LogUtils.debug(msgCode.getCode("M0013"),Main.class);
				
				LogUtils.debug(msgCode.getCode("M0014"),Main.class);
				managementConstraint.createFk(dbInform);
				LogUtils.debug(msgCode.getCode("M0015"),Main.class);
			}	
		}
		

		//pool 삭제
		shutDownPool();
		LogUtils.info(msgCode.getCode("M0016"),Main.class);
	}
	
	//pool 생성
	private static void createPool() throws Exception {
		//DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG, Constant.POOLNAME.SOURCE_DDL.name(), 1);
//		DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG, Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_SELECT_ON_PARALLEL);
//		
//		if(ConfigInfo.TAR_CONSTRAINT_DDL || ConfigInfo.SRC_INCLUDE_DATA_EXPORT) {
//			
//			int intTarConnCount = ConfigInfo.TAR_CONN_COUNT;
//			if(ConfigInfo.SRC_SELECT_ON_PARALLEL > intTarConnCount) {
//				intTarConnCount = ConfigInfo.SRC_SELECT_ON_PARALLEL;
//			}
//			DBCPPoolManager.setupDriver(ConfigInfo.TAR_DB_CONFIG, Constant.POOLNAME.TARGET.name(), intTarConnCount);
//		}
		if(ConfigInfo.SRC_INCLUDE_DATA_EXPORT || ConfigInfo.SRC_DDL_EXPORT || checkQueryXml()) {
			DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG
					, Constant.POOLNAME.SOURCE.name()
					, ConfigInfo.SRC_SELECT_ON_PARALLEL
			);
		}
		if(ConfigInfo.TAR_CONSTRAINT_DDL || ConfigInfo.DB_WRITER_MODE) {
			DBCPPoolManager.setupDriver(ConfigInfo.TAR_DB_CONFIG
					, Constant.POOLNAME.TARGET.name()
					, ConfigInfo.SRC_SELECT_ON_PARALLEL > ConfigInfo.TAR_CONN_COUNT 
					  ? ConfigInfo.SRC_SELECT_ON_PARALLEL
					  : ConfigInfo.TAR_CONN_COUNT
		    );
		}
	}
	
	// shutdown pool
	private static void shutDownPool() throws Exception {
		DBCPPoolManager.shutdownDriver(Constant.POOLNAME.SOURCE.name());
		DBCPPoolManager.shutdownDriver(Constant.POOLNAME.TARGET.name());
	}
	
	private static void makeDirectory() throws Exception {
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_PATH);
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_PATH+"data/");
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_PATH+"ddl/");
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_PATH+"rebuild/");
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_PATH+"result/");		
	}
	
	private static File checkDirectory(String strDirectory) throws Exception {
		File dir = new File(strDirectory);
		if(!dir.exists()){
			LogUtils.info(String.format(msgCode.getCode("C0025"), dir.getPath()), Main.class);
			if(dir.mkdirs()) {
				LogUtils.info(String.format(msgCode.getCode("C0026"), dir.getPath()), Main.class);
			} else {
				LogUtils.error(String.format(msgCode.getCode("C0027"), dir.getPath()), Main.class);
				System.exit(Constant.ERR_CD.FAILED_CREATE_DIR_ERR);
			}
		}
		
		return dir;
	}
	
	private static void makeSqlFile(TargetPgDDL dbInform) throws Exception {
		
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_PATH+"rebuild/");
		
		//TargetPgDDL dbInform = new TargetPgDDL();
		
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_PATH + "rebuild/fk_drop.sql", dbInform.getFkDropList());
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_PATH + "rebuild/idx_drop.sql", dbInform.getIdxDropList());
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_PATH + "rebuild/idx_create.sql", dbInform.getIdxCreateList());
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_PATH + "rebuild/fk_create.sql", dbInform.getFkCreateList());
	}
	
	private static Boolean checkQueryXml() throws Exception {
		if(!ConfigInfo.SRC_FILE_QUERY_DIR_PATH.equals("")) {
			File f = new File(ConfigInfo.SRC_FILE_QUERY_DIR_PATH);
			if(f.exists() && !f.isDirectory()) return true;
			else return false;
		}else {
			return false;
		}
	}
	
}
