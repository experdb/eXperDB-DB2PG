package com.k4m.experdb.db2pg.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.impl.MetaExtractWorker.WORK_TYPE;

public class DBUtils {
	
	public static List<String> getTableNames(boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<String> tableNames = null;
		try {
			Map<String,Object> params = new HashMap<String,Object>();
			LogUtils.info("[START_GET_TABLE_NAMES]",DBUtils.class);
			if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.ORA)){
				params.put("OWNER", dbConfigInfo.SCHEMA_NAME);
				params.put("TABLE_ONLY", tableOnly?"AND OBJECT_TYPE IN ('TABLE')":"AND OBJECT_TYPE IN ('TABLE','VIEW')");
			} else if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MSS)) {
				params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
			} else if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.ASE)) {
				params.put("USER_NAME", dbConfigInfo.SCHEMA_NAME);
			} else if(dbConfigInfo.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
				params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
//				params.put("TABLE_ONLY", tableOnly?"WHERE table_type IN ('BASE TABLE')":"WHERE table_type IN ('BASE TABLE','VIEW')");
				params.put("TABLE_ONLY", "AND table_type IN ('BASE TABLE')");
			}
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName,new MetaExtractWork(WORK_TYPE.GET_TABLE_NAMES, params));
			mew.run();
			tableNames = (List<String>)mew.getResult();
					
		} catch(Exception e){
			LogUtils.error(e.getMessage(),DBUtils.class);
		} finally {
			LogUtils.info("[END_GET_TABLE_NAMES]",DBUtils.class);
		}
		return tableNames;
	}
	
}
