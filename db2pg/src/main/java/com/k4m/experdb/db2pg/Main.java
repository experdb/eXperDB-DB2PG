package com.k4m.experdb.db2pg;

import java.io.File;

import org.apache.log4j.LogManager;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ArgsParser;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.DdlConverter;
import com.k4m.experdb.db2pg.rebuild.MakeSqlFile;
import com.k4m.experdb.db2pg.rebuild.TargetPgDDL;
import com.k4m.experdb.db2pg.unload.Unloader;



public class Main {
	public static void main(String[] args) throws Exception {
		ArgsParser argsParser = new ArgsParser();
		argsParser.parse(args);
		LogUtils.setVerbose(ConfigInfo.VERBOSE);
		LogManager.getRootLogger().setLevel(ConfigInfo.LOG_LEVEL);
		
		LogUtils.info("[DB2PG_START]",Main.class);
		File dir = new File(ConfigInfo.OUTPUT_DIRECTORY);
		if(!dir.exists()){
			while(!dir.mkdirs()){
				try {
					Thread.sleep(10);
				} catch(Exception e) {
				}
			}
		}
		if(ConfigInfo.SRC_DDL_EXPORT) {
			LogUtils.debug("[SRC_DDL_EXPORT_START]",Main.class);
			DdlConverter ddlConv = new DdlConverter();
			ddlConv.start();
			LogUtils.debug("[SRC_DDL_EXPORT_END]",Main.class);
		}
		
		if(ConfigInfo.SRC_EXPORT) {
			LogUtils.debug("[SRC_EXPORT_START]",Main.class);
			Unloader loader = new Unloader();
			loader.start();	
			LogUtils.debug("[SRC_EXPORT_END]",Main.class);
		}
		
		if(ConfigInfo.PG_CONSTRAINT_EXTRACT) {
			LogUtils.debug("[PG_CONSTRAINT_EXTRACT_START]",Main.class);
			TargetPgDDL dbInform = new TargetPgDDL();
			dir = new File(ConfigInfo.OUTPUT_DIRECTORY+"rebuild/");
			if(!dir.exists()){
				while(!dir.mkdirs()){
					try {
						Thread.sleep(10);
					} catch(Exception e) {
					}
				}
			}
			MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/fk_drop.sql", dbInform.getFkDropList());
			MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/idx_drop.sql", dbInform.getIdxDropList());
			MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/idx_create.sql", dbInform.getIdxCreateList());
			MakeSqlFile.listToSqlFile(ConfigInfo.OUTPUT_DIRECTORY + "rebuild/fk_create.sql", dbInform.getFkCreateList());
			LogUtils.debug("[PG_CONSTRAINT_EXTRACT_END]",Main.class);
		}
		
		LogUtils.info("[DB2PG_END]",Main.class);
	}
	
}
