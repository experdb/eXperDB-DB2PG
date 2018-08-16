package com.k4m.experdb.db2pg.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.StopWatch;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker.WORK_TYPE;

public class DBUtils {
	public static List<String> getTableNames(boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<String> tableNames = null;
		try {
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			Map<String,Object> params = new HashMap<String,Object>();
			LogUtils.info("[START_GET_TABLE_NAMES]",DBUtils.class);
			
			params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
			params.put("TABLE_ONLY", tableOnly);
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName,new MetaExtractWork(WORK_TYPE.GET_TABLE_NAMES, params));
			mew.run();
			tableNames = (List<String>)mew.getResult();
			stopWatch.stop();
			LogUtils.debug("[GET_TABLE_NAMES_ELAPSED_TIME] "+dbConfigInfo.DB_TYPE+" " + stopWatch.getTime()+"ms",DBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),DBUtils.class);
		} finally {
			LogUtils.info("[END_GET_TABLE_NAMES]",DBUtils.class);
		}
		return tableNames;
	}
	
}
