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
import com.k4m.experdb.db2pg.unload.ExecuteDataTransfer;
import com.k4m.experdb.db2pg.unload.Unloader;



public class Main {
	public static void main(String[] args) throws Exception {
		ArgsParser argsParser = new ArgsParser();
		argsParser.parse(args);
		LogUtils.setVerbose(ConfigInfo.VERBOSE);
		LogManager.getRootLogger().setLevel(ConfigInfo.LOG_LEVEL);
		
		checkConfigInfo();
		
		//pool 생성
		createPool();
		
		LogUtils.info("[DB2PG_START]",Main.class);
		
		//check output directory 
		//checkDirectory(ConfigInfo.OUTPUT_DIRECTORY);
		makeDirectory();
		
		if(ConfigInfo.SRC_DDL_EXPORT) {
			
			LogUtils.debug("[SRC_DDL_EXPORT_START]",Main.class);
			DDLConverter ddlConv = DDLConverter.getInstance();
			ddlConv.start();
			LogUtils.debug("[SRC_DDL_EXPORT_END]",Main.class);
		}
		
		if(ConfigInfo.SRC_EXPORT) {
			TargetPgDDL dbInform = new TargetPgDDL();
			ExecuteDataTransfer executeDataTransfer = new ExecuteDataTransfer();
			
			LogUtils.debug("[PG_CONSTRAINT_EXTRACT_START]",Main.class);
			makeSqlFile(dbInform);
			LogUtils.debug("[PG_CONSTRAINT_EXTRACT_END]",Main.class);
			
			if(ConfigInfo.TAR_DROP_CREATE_FK) {
				LogUtils.debug("[DROP_FK_START]",Main.class);
				executeDataTransfer.dropFk(dbInform);
				LogUtils.debug("[DROP_FK_END]",Main.class);
			}
			
			if(ConfigInfo.TAR_DROP_CREATE_INDEX) {
				LogUtils.debug("[DROP_INDEX_START]",Main.class);
				executeDataTransfer.dropIndex(dbInform);
				LogUtils.debug("[DROP_INDEX_END]",Main.class);
			}
			
			LogUtils.debug("[SRC_EXPORT_START]",Main.class);
			Unloader loader = new Unloader();
			loader.start();	
			LogUtils.debug("[SRC_EXPORT_END]",Main.class);
			
			if(ConfigInfo.TAR_DROP_CREATE_FK) {
				LogUtils.debug("[CREATE_FK_START]",Main.class);
				executeDataTransfer.createFk(dbInform);
				LogUtils.debug("[CREATE_FK_END]",Main.class);
			}
			
			if(ConfigInfo.TAR_DROP_CREATE_INDEX) {
				LogUtils.debug("[CREATE_INDEX_START]",Main.class);
				executeDataTransfer.createIndex(dbInform);
				LogUtils.debug("[CREATE_INDEX_END]",Main.class);
			}
		}
		

		//pool 삭제
		shutDownPool();
		LogUtils.info("[DB2PG_END]",Main.class);
	}
	
	//pool 생성
	private static void createPool() throws Exception {
		//DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG, Constant.POOLNAME.SOURCE_DDL.name(), 1);
		DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG, Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_TABLE_SELECT_PARALLEL);
		
		if(ConfigInfo.PG_CONSTRAINT_EXTRACT || ConfigInfo.SRC_EXPORT) {
			
			int intTarConnCount = ConfigInfo.TAR_CONN_COUNT;
			if(ConfigInfo.SRC_TABLE_SELECT_PARALLEL > intTarConnCount) {
				intTarConnCount = ConfigInfo.SRC_TABLE_SELECT_PARALLEL;
			}
			DBCPPoolManager.setupDriver(ConfigInfo.TAR_DB_CONFIG, Constant.POOLNAME.TARGET.name(), intTarConnCount);
		}
	}
	
	//pool 삭제
	private static void shutDownPool() throws Exception {
		DBCPPoolManager.shutdownDriver(Constant.POOLNAME.SOURCE.name());
		DBCPPoolManager.shutdownDriver(Constant.POOLNAME.TARGET.name());
		
	}
	
	private static void checkConfigInfo() throws Exception {
		if(ConfigInfo.SRC_DDL_EXPORT) {
			if(ConfigInfo.TAR_DB_CONFIG.CHARSET == null || ConfigInfo.TAR_DB_CONFIG.CHARSET.equals("")) {
				System.exit(Constant.ERR_CD.CONFIG_NOT_FOUND);
			}
		}
		
		
	}
	
	private static void makeDirectory() throws Exception {
		
		checkDirectory(ConfigInfo.OUTPUT_DIRECTORY);
		checkDirectory(ConfigInfo.OUTPUT_DIRECTORY+"data/");
		checkDirectory(ConfigInfo.OUTPUT_DIRECTORY+"ddl/");
		checkDirectory(ConfigInfo.OUTPUT_DIRECTORY+"rebuild/");
		checkDirectory(ConfigInfo.OUTPUT_DIRECTORY+"result/");		
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
		
		checkDirectory(ConfigInfo.OUTPUT_DIRECTORY+"rebuild/");
		
		//TargetPgDDL dbInform = new TargetPgDDL();
		
		MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/fk_drop.sql", dbInform.getFkDropList());
		MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/idx_drop.sql", dbInform.getIdxDropList());
		MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/idx_create.sql", dbInform.getIdxCreateList());
		MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/fk_create.sql", dbInform.getFkCreateList());
	}
	
}
