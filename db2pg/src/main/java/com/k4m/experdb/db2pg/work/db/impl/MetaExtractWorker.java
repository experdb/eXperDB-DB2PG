package com.k4m.experdb.db2pg.work.db.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;

import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.mapper.MetaExtractMapper;
import com.k4m.experdb.db2pg.work.db.DBWorker;

public final class MetaExtractWorker extends DBWorker {
	private SqlSession sqlSession;
	private String poolName;
	private MetaExtractMapper mapper;
	private boolean stop;
	private MetaExtractWork work;
	private Object result;

	public MetaExtractWorker(String poolName, MetaExtractWork work) throws Exception {
		super();
		this.poolName = poolName;
		this.work = work;
		sqlSession = DBCPPoolManager.getSession(poolName);
		mapper = sqlSession.getMapper(MetaExtractMapper.class);
		stop = true;
	}

	@Override
	public void run() {
		try {
			isRunning = true;
			stop = false;
			if (work.params == null) {
				work.params = new HashMap<String, Object>();
			}
			DBConfigInfo dbconf = DBCPPoolManager.getConfigInfo(poolName);
			work.params.put("DB_VER", dbconf.DB_VER);
			work.params.put("DB_MAJOR_VER", dbconf.DB_MAJOR_VER);
			work.params.put("DB_MINOR_VER", dbconf.DB_MINOR_VER);
			switch (work.type) {
			case GET_AUTOINCREMENT_INFORM:
				result = mapper.getAutoincrementInform(work.params);
				break;
			case GET_COLUMN_INFORM:
				result = mapper.getColumnInform(work.params);
				break;
			case GET_CONSTRAINT_INFORM:
				result = mapper.getConstraintInform(work.params);
				break;
			case GET_CREATE_TABLE:
				result = mapper.getCreateTable(work.params);
				break;
			case GET_KEY_INFORM:
				result = mapper.getKeyInform(work.params);
				break;
			case GET_SOURCE_TABLE_DATA:
				result = mapper.getSourceTableData(work.params);
				break;
			case GET_TABLE_INFORM:
				result = mapper.getTableInform(work.params);
				break;
			case GET_TABLE_NAMES:
				result = mapper.getTableNames(work.params);
				break;
			case GET_PG_CURRENT_SCHEMA:
				result = mapper.getPgCurrentSchema();
				break;
			case GET_PG_FK_DDL:
				result = mapper.getPgFkDdl();
				break;
			case GET_PG_IDX_DDL:
				result = mapper.getPgIdxDdl();
				break;
			}
			stop();
		} catch (Exception e) {
			this.exception = e;
			hasException = true;
		} finally {
			isRunning = false;
		}
	}

	@Override
	public void stop() {
		stop = true;
		sqlSession.close();
	}

	public Object getResult() {
		return result;
	}
	
	public List<?> getListResult() {
		return (List<?>) result;
	}
	
	public Map<?,?> getMapResult() {
		return (Map<?,?>)result;
	}

	public String getPoolName() {
		return poolName;
	}

	public enum WORK_TYPE {
		GET_TABLE_NAMES, GET_SOURCE_TABLE_DATA, GET_CREATE_TABLE, GET_TABLE_INFORM, GET_COLUMN_INFORM, GET_CONSTRAINT_INFORM, GET_KEY_INFORM, GET_AUTOINCREMENT_INFORM, GET_PG_CURRENT_SCHEMA, GET_PG_IDX_DDL, GET_PG_FK_DDL
	}

}
