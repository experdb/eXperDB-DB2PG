package com.k4m.experdb.db2pg.rebuild;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;

public class MakeSqlFile {
	public static <T> boolean listToSqlFile(String filepath,List<T> list) {
		boolean check = true;
		LogUtils.info(String.format("[LIST_TO_FILE_START] %s", filepath),MakeSqlFile.class);
		try {
			File file = new File(filepath);
			FileOutputStream fos = new FileOutputStream(file);
			fos.write(String.format("SET client_encoding TO '%s';\n\n",ConfigInfo.TAR_DB_CHARSET).getBytes(ConfigInfo.FILE_CHARACTERSET));
        	fos.write("\\set ON_ERROR_STOP OFF\n".getBytes());
        	fos.write("\\set ON_ERROR_ROLLBACK OFF\n\n".getBytes());
        	fos.write(String.format("\\echo [FILE_NAME] %s\n\n"
        			,filepath.substring(filepath.lastIndexOf('/')+1
        			,filepath.lastIndexOf('.'))).getBytes(ConfigInfo.FILE_CHARACTERSET));
        	fos.write("\\timing \n".getBytes(ConfigInfo.FILE_CHARACTERSET));
			for(T obj : list) {
				String sql = obj.toString();
				fos.write(String.format("\\echo [SQL] \"%s\"\n",sql.replace("\"", "")).getBytes(ConfigInfo.FILE_CHARACTERSET));
				if(sql.startsWith("CREATE INDEX")){
					int onIdx = sql.indexOf("ON");
					sql =  String.format("%s%s.%s", sql.substring(0,onIdx+3),ConfigInfo.TAR_SCHEMA,sql.substring(onIdx+3));
				} else if (sql.startsWith("DROP INDEX")){
					int onIdx = sql.indexOf("INDEX");
					sql = String.format("%s\"%s\".%s", sql.substring(0,onIdx+6),ConfigInfo.TAR_SCHEMA,sql.substring(onIdx+6));
				}
				fos.write(sql.getBytes());
				fos.write('\n');
			}
			fos.flush();
			fos.close();
		} catch (Exception e) {
			LogUtils.error("[LIST_TO_FILE_ERROR]",MakeSqlFile.class,e);
			check = false;
		} finally {
			LogUtils.info(String.format("[LIST_TO_FILE_END] %s", filepath),MakeSqlFile.class);
		}
		
		return check;
	}
	
	/**
	 * Create Sql Integration File
	 * */
//	public static boolean createSqlIntegrateFile(String filepath,String... sqlFiles) {
//		boolean check = true;
//		LogUtils.info(String.format("[CREATE_SQL_INTEGRATE_FILE_START] %s", filepath),MakeSqlFile.class);
//		
//		try {
//			File file = new File(filepath);
//			FileOutputStream fos = new FileOutputStream(file);
//			fos.write(String.format("SET client_encoding TO '%s';\n\n",ConfigFileInfo.TAR_DB_CHARSET).getBytes());
//        	fos.write("\\set ON_ERROR_STOP OFF\n\n".getBytes());
//        	fos.write("\\set ON_ERROR_ROLLBACK OFF\n\n".getBytes());
//			for(String sqlFile : sqlFiles) {
////				fos.write(String.format("\\echo %s START\n",filepath.substring(filepath.lastIndexOf('/')+1)).getBytes());
//				fos.write(String.format("\\i %s\n",sqlFile).getBytes());
////				fos.write(String.format("\\echo %s END\n\n",filepath.substring(filepath.lastIndexOf('/')+1)).getBytes());
////				fos.write(obj.toString().getBytes());
////				fos.write('\n');
//			}
//			fos.flush();
//			fos.close();
//		} catch (Exception e) {
//			LogUtils.error("[CREATE_SQL_INTEGRATE_FILE_ERROR]",MakeSqlFile.class,e);
//			check = false;
//		} finally {
//			LogUtils.info(String.format("[CREATE_SQL_INTEGRATE_FILE_END] %s", filepath),MakeSqlFile.class);
//		}
//		return check;
//	}
	
//	public static boolean createPsqlShellFile(String filepath,String... sqlFiles) {
//		boolean check = true;
//		LogUtils.info(String.format("[CREATE_PSQL_SHELL_FILE_START] %s", filepath),MakeSqlFile.class);
//		String filename = filepath.substring(filepath.lastIndexOf('/')+1);
//		try {
//			File file = new File(filepath);
//			FileOutputStream fos = new FileOutputStream(file);
//			fos.write("#!/bin/sh\n".getBytes());
//			fos.write(String.format("DB_HOST=%s\n",ConfigFileInfo.TAR_HOST).getBytes());
//			fos.write(String.format("DB_PORT=%d\n",ConfigFileInfo.TAR_PORT).getBytes());
//			fos.write(String.format("DB_OWNER=%s\n",ConfigFileInfo.TAR_USER).getBytes());
//			fos.write(String.format("DB_NAME=%s\n",ConfigFileInfo.TAR_DATABASE).getBytes());
//			fos.write("LOG_DIR=.\n".getBytes());
//			fos.write("LOG_NAME=rebuilderpg.log\n".getBytes());
//			fos.write("mkdir -p $LOG_DIR\n".getBytes());
//			fos.write(String.format("SQL_DIR=.\n\n").getBytes());
//			fos.write("CREATE=0\n".getBytes());
//			fos.write("DROP=0\n".getBytes());
//			fos.write("while getopts \"cd\" opt; do\n".getBytes());
//			fos.write("\tcase \"$opt\" in\n".getBytes());
//			fos.write("\tc) CREATE=1 ;;\n".getBytes());
//			fos.write("\td) DROP=1;;\n".getBytes());
//			fos.write("\t*) die \"Unknown error while processing options\";;\n".getBytes());
//			fos.write("esac\n".getBytes());
//			fos.write("done\n\n".getBytes());
//			
//			
//			
//			for(String sqlFile : sqlFiles) {
//				if(sqlFile.equals("create.sql")){
//					fos.write("if [ $CREATE -eq 1 ]; then\n".getBytes());
//				} else if(sqlFile.equals("drop.sql")){
//					//clear logfile contents
//					fos.write("if [ $DROP -eq 1 ]; then\n".getBytes());
//					fos.write("\techo > ".getBytes());
//					fos.write("$LOG_DIR/".getBytes());
//					fos.write("$LOG_NAME".getBytes());
//					fos.write("\n".getBytes());
//				}
//				
//				fos.write(String.format("\tpsql -h $DB_HOST -p $DB_PORT -U $DB_OWNER -d $DB_NAME -f $SQL_DIR/%s >> ",sqlFile).getBytes());
//				fos.write("$LOG_DIR/".getBytes());
//				fos.write("$LOG_NAME".getBytes());
//				fos.write("\n".getBytes());
//				fos.write("fi\n".getBytes());
//				
//			}
//			fos.flush();
//			fos.close();
//		} catch (Exception e) {
//			LogUtils.error("[CREATE_PSQL_SHELL_FILE_ERROR]",MakeSqlFile.class,e);
//			check = false;
//		} finally {
//			LogUtils.info(String.format("[CREATE_PSQL_SHELL_FILE_END] %s", filepath),MakeSqlFile.class);
//		}
//		return check;
//	}
}
