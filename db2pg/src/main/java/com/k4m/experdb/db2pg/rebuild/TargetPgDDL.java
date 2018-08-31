package com.k4m.experdb.db2pg.rebuild;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker.WORK_TYPE;

public class TargetPgDDL {
	private List<String> idxCreateList;
	private List<String> idxDropList;
	private List<String> fkCreateList;
	private List<String> fkDropList;
	
	public TargetPgDDL(){
		idxCreateList = new ArrayList<String>();
		idxDropList = new ArrayList<String>();
		fkCreateList = new ArrayList<String>();
		fkDropList = new ArrayList<String>();
		
		DBConfigInfo tarPgConf = ConfigInfo.SRC_DB_CONFIG;
		try {
			DBCPPoolManager.setupDriver(tarPgConf, Constant.POOLNAME.TARGET.name(), 1);
			LogUtils.info("[GET_DATABASE_INFORM_START]",TargetPgDDL.class);
			try {
				LogUtils.info("[GET_CURRENT_SCHEMA_START]",TargetPgDDL.class);
				MetaExtractWorker mew = new MetaExtractWorker(Constant.POOLNAME.TARGET.name(), new MetaExtractWork(WORK_TYPE.GET_PG_CURRENT_SCHEMA));
				mew.run();
				tarPgConf.SCHEMA_NAME = (String)mew.getResult();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				LogUtils.info("[GET_CURRENT_SCHEMA_END]",TargetPgDDL.class);
			}
			try {
				LogUtils.info("[GET_INDEX_INFORM_START]",TargetPgDDL.class);
				MetaExtractWorker mew = new MetaExtractWorker(Constant.POOLNAME.TARGET.name(), new MetaExtractWork(WORK_TYPE.GET_PG_IDX_DDL));
				mew.run();
				@SuppressWarnings("unchecked")
				List<Map<String, Object>> results = (List<Map<String, Object>>)mew.getListResult();
				for (Map<String, Object> result : results) {
					idxCreateList.add((String)result.get("CREATE_DDL_SCRIPT"));
					idxDropList.add((String)result.get("DROP_DDL_SCRIPT"));
				}
			} catch (Exception e){
				throw(new Exception("[GET_INDEX_INFORM_ERROR]",e));
			} finally {
				LogUtils.info("[GET_INDEX_INFORM_END]",TargetPgDDL.class);
			}
			try {
				LogUtils.info("[GET_FK_INFORM_START]",TargetPgDDL.class);
				MetaExtractWorker mew = new MetaExtractWorker(Constant.POOLNAME.TARGET.name(), new MetaExtractWork(WORK_TYPE.GET_PG_FK_DDL));
				mew.run();
				@SuppressWarnings("unchecked")
				List<Map<String, Object>> results = (List<Map<String, Object>>)mew.getListResult();
				for (Map<String, Object> result : results){
					fkCreateList.add((String)result.get("CREATE_DDL_SCRIPT"));
					fkDropList.add((String)result.get("DROP_DDL_SCRIPT"));
				}
			} catch (Exception e){
				throw(new Exception("[GET_FK_INFORM_ERROR]",e));
			} finally {
				LogUtils.info("[GET_FK_INFORM_END]",TargetPgDDL.class);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			LogUtils.info("[GET_DATABASE_INFORM_END]",TargetPgDDL.class);
		}
	}

	public List<String> getIdxCreateList() {
		return idxCreateList;
	}

	public List<String> getIdxDropList() {
		return idxDropList;
	}

	public List<String> getFkCreateList() {
		return fkCreateList;
	}

	public List<String> getFkDropList() {
		return fkDropList;
	}
	
	
}
