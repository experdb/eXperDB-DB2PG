package com.k4m.experdb.db2pg;

import java.io.File;

import org.apache.log4j.LogManager;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ArgsParser;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.DDLConverter;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.rebuild.MakeSqlFile;
import com.k4m.experdb.db2pg.rebuild.TargetPgDDL;
import com.k4m.experdb.db2pg.unload.ManagementConstraint;
import com.k4m.experdb.db2pg.unload.Unloader;



public class Main {
	public static void main(String[] args) throws Exception {
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
		
		LogUtils.info("[DB2PG_START]",Main.class);
		
		// check output directory 
		// checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH);
		makeDirectory();
		
		if(ConfigInfo.SRC_DDL_EXPORT) {
			LogUtils.debug("[SRC_DDL_EXPORT_START]",Main.class);
			DDLConverter ddlConv = DDLConverter.getInstance();
			ddlConv.start();
			LogUtils.debug("[SRC_DDL_EXPORT_END]",Main.class);
		}
		
		if(ConfigInfo.TAR_CONSTRAINT_DDL) {
			TargetPgDDL targetPgDDL = new TargetPgDDL();
			makeSqlFile(targetPgDDL);
		}
		
		if(ConfigInfo.SRC_INCLUDE_DATA_EXPORT) {
			TargetPgDDL dbInform = null ;
			if(ConfigInfo.DB_WRITER_MODE ) dbInform = new TargetPgDDL();
			
			ManagementConstraint managementConstraint = new ManagementConstraint();
			
			if(ConfigInfo.DB_WRITER_MODE ) {
				LogUtils.debug("[PG_CONSTRAINT_EXTRACT_START]",Main.class);
				makeSqlFile(dbInform);
				LogUtils.debug("[PG_CONSTRAINT_EXTRACT_END]",Main.class);
			}
						
			if(ConfigInfo.DB_WRITER_MODE && ConfigInfo.TAR_CONSTRAINT_REBUILD) {
				LogUtils.debug("[DROP_FK_START]",Main.class);
				managementConstraint.dropFk(dbInform);
				LogUtils.debug("[DROP_FK_END]",Main.class);
				
				LogUtils.debug("[DROP_INDEX_START]",Main.class);
				managementConstraint.dropIndex(dbInform);
				LogUtils.debug("[DROP_INDEX_END]",Main.class);
			}
		
			LogUtils.debug("[SRC_INCLUDE_DATA_EXPORT_START]",Main.class);
			Unloader loader = new Unloader();
			loader.start();	
			LogUtils.debug("[SRC_INCLUDE_DATA_EXPORT_END]",Main.class);

			if(ConfigInfo.DB_WRITER_MODE && ConfigInfo.TAR_CONSTRAINT_REBUILD) {					
				LogUtils.debug("[CREATE_INDEX_START]",Main.class);
				managementConstraint.createIndex(dbInform);
				LogUtils.debug("[CREATE_INDEX_END]",Main.class);
				
				LogUtils.debug("[CREATE_FK_START]",Main.class);
				managementConstraint.createFk(dbInform);
				LogUtils.debug("[CREATE_FK_END]",Main.class);
			}	
		}
		

		//pool 삭제
		shutDownPool();
		LogUtils.info("[DB2PG_END]",Main.class);
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
		if(ConfigInfo.SRC_INCLUDE_DATA_EXPORT || ConfigInfo.SRC_DDL_EXPORT) {
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
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH);
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH+"data/");
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH+"ddl/");
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH+"rebuild/");
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH+"result/");		
	}
	
	private static File checkDirectory(String strDirectory) throws Exception {
		File dir = new File(strDirectory);
		if(!dir.exists()){
			LogUtils.info(String.format("%s directory is not existed.", dir.getPath()), Main.class);
			if(dir.mkdirs()) {
				LogUtils.info(String.format("Success to create %s directory.", dir.getPath()), Main.class);
			} else {
				LogUtils.error(String.format("Failed to create %s directory.", dir.getPath()), Main.class);
				System.exit(Constant.ERR_CD.FAILED_CREATE_DIR_ERR);
			}
		}
		
		return dir;
	}
	
	private static void makeSqlFile(TargetPgDDL dbInform) throws Exception {
		
		checkDirectory(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH+"rebuild/");
		
		//TargetPgDDL dbInform = new TargetPgDDL();
		
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH + "rebuild/fk_drop.sql", dbInform.getFkDropList());
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH + "rebuild/idx_drop.sql", dbInform.getIdxDropList());
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH + "rebuild/idx_create.sql", dbInform.getIdxCreateList());
		MakeSqlFile.listToSqlFile(ConfigInfo.SRC_FILE_OUTPUT_DIR_PATH + "rebuild/fk_create.sql", dbInform.getFkCreateList());
	}
	
}
