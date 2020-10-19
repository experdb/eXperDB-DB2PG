package com.k4m.experdb.db2pg.rebuild;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.db.impl.WORK_TYPE;

public class TargetPgDDL {
	static MsgCode msgCode = new MsgCode();
	private List<String> idxCreateList;
	private List<String> idxDropList;
	private List<String> fkCreateList;
	private List<String> fkDropList;
	
	public TargetPgDDL(){

		idxCreateList = new ArrayList<String>();
		idxDropList = new ArrayList<String>();
		fkCreateList = new ArrayList<String>();
		fkDropList = new ArrayList<String>();
		
		//DBConfigInfo tarPgConf = ConfigInfo.SRC_DB_CONFIG;
		
		DBConfigInfo tarPgConf = ConfigInfo.TAR_DB_CONFIG;
		try {
			//DBCPPoolManager.setupDriver(tarPgConf, Constant.POOLNAME.TARGET.name(), 1);
			LogUtils.info(msgCode.getCode("C0114"),TargetPgDDL.class);
			try {
				LogUtils.info(msgCode.getCode("C0115"),TargetPgDDL.class);
				MetaExtractWorker mew = new MetaExtractWorker(Constant.POOLNAME.TARGET.name(), new MetaExtractWork(WORK_TYPE.GET_PG_CURRENT_SCHEMA));
				mew.run();
				tarPgConf.SCHEMA_NAME = (String)mew.getResult();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				LogUtils.info(msgCode.getCode("C0116"),TargetPgDDL.class);
			}
			try {
				LogUtils.info(msgCode.getCode("C0117"),TargetPgDDL.class);
				MetaExtractWorker mew = new MetaExtractWorker(Constant.POOLNAME.TARGET.name(), new MetaExtractWork(WORK_TYPE.GET_PG_IDX_DDL));
				mew.run();
				@SuppressWarnings("unchecked")
				List<Map<String, Object>> results = (List<Map<String, Object>>)mew.getListResult();
				for (Map<String, Object> result : results) {
					idxCreateList.add((String)result.get("CREATE_DDL_SCRIPT"));
					idxDropList.add((String)result.get("DROP_DDL_SCRIPT"));
				}
			} catch (Exception e){
				throw(new Exception(msgCode.getCode("C0118"),e));
			} finally {
				LogUtils.info(msgCode.getCode("C0119"),TargetPgDDL.class);
			}
			try {
				LogUtils.info(msgCode.getCode("C0120"),TargetPgDDL.class);
				MetaExtractWorker mew = new MetaExtractWorker(Constant.POOLNAME.TARGET.name(), new MetaExtractWork(WORK_TYPE.GET_PG_FK_DDL));
				mew.run();
				@SuppressWarnings("unchecked")
				List<Map<String, Object>> results = (List<Map<String, Object>>)mew.getListResult();
				for (Map<String, Object> result : results){
					fkCreateList.add((String)result.get("create_ddl_script"));
					fkDropList.add((String)result.get("drop_ddl_script"));
				}
			} catch (Exception e){
				throw(new Exception(msgCode.getCode("C0121"),e));
			} finally {
				LogUtils.info(msgCode.getCode("C0122"),TargetPgDDL.class);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			LogUtils.info(msgCode.getCode("C0123"),TargetPgDDL.class);
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
