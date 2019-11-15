package com.k4m.experdb.db2pg.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.StopWatch;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.db.impl.WORK_TYPE;

public class DBUtils {
	static MsgCode msgCode = new MsgCode();
	public static List<String> getTableNames(boolean tableDdlOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<String> tableNames = null;
		try {
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			Map<String,Object> params = new HashMap<String,Object>();
			LogUtils.info(msgCode.getCode("C0087"),DBUtils.class);
			
			params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);
			params.put("SRC_TABLE_DDL", tableDdlOnly);
			
			if(dbConfigInfo.DB_TYPE.equals("CUB")){
				params.put("TABLE_SCHEMA", dbConfigInfo.USERID.toUpperCase());
			}
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName,new MetaExtractWork(WORK_TYPE.GET_TABLE_NAMES, params));
			mew.run();
			
			tableNames = (List<String>)mew.getResult();
			LogUtils.debug(String.format(msgCode.getCode("C0088"),tableNames),DBUtils.class);
			stopWatch.stop();
			LogUtils.debug(String.format(msgCode.getCode("C0089"),dbConfigInfo.DB_TYPE,stopWatch.getTime()),DBUtils.class);

		} catch(Exception e){
			System.out.println("error");
			LogUtils.error(e.getMessage(),DBUtils.class);
		} finally {
			LogUtils.info(msgCode.getCode("C0090"),DBUtils.class);
		}
		return tableNames;
	}
	
}
