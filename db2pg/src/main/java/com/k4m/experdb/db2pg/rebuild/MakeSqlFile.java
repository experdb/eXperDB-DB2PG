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
			fos.write(String.format("SET client_encoding TO '%s';\n\n",ConfigInfo.TAR_DB_CONFIG.CHARSET).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
        	fos.write("\\set ON_ERROR_STOP OFF\n".getBytes());
        	fos.write("\\set ON_ERROR_ROLLBACK OFF\n\n".getBytes());
        	fos.write(String.format("\\echo [FILE_NAME] %s\n\n"
        			,filepath.substring(filepath.lastIndexOf('/')+1
        			,filepath.lastIndexOf('.'))).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
        	fos.write("\\timing \n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			for(T obj : list) {
				String sql = obj.toString();
				fos.write(String.format("\\echo [SQL] \"%s\"\n",sql.replace("\"", "")).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				if(sql.startsWith("CREATE INDEX")){
					int onIdx = sql.indexOf("ON");
					sql =  String.format("%s%s.%s", sql.substring(0,onIdx+3),ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME,sql.substring(onIdx+3));
				} else if (sql.startsWith("DROP INDEX")){
					int onIdx = sql.indexOf("INDEX");
					sql = String.format("%s\"%s\".%s", sql.substring(0,onIdx+6),ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME,sql.substring(onIdx+6));
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
	
}
