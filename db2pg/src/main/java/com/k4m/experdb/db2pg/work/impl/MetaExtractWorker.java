package com.k4m.experdb.db2pg.work.impl;

import java.util.List;

import org.apache.ibatis.session.SqlSession;

import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.mapper.MetaExtractMapper;
import com.k4m.experdb.db2pg.work.DBWorker;

public class MetaExtractWorker extends DBWorker {
	private SqlSession sqlSession;
	private String poolName;
	private MetaExtractMapper mapper;
	private boolean stop;
	private List<MetaExtractWork> works;
	private Object result;
	
	public MetaExtractWorker(String poolName, List<MetaExtractWork> works) throws Exception {
		super();
		this.poolName = poolName;
		this.works = works;
		sqlSession = DBCPPoolManager.getSession(poolName);
		mapper = sqlSession.getMapper(MetaExtractMapper.class);
		stop = false;
	}

	@Override
	public void run() {
		try {
			isRunning = true;
			for(MetaExtractWork work : works) {
				switch(work.type) {
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
				case GET_TABLE_NAME:
					result = mapper.getTableName(work.params);
					break;
				case GET_PG_CURRENT_SCHEMA:
					result = mapper.getPgCurrentSchema(work.params);
					break;
				case GET_PG_FK_DDL:
					result = mapper.getPgFkDdl(work.params);
					break;
				case GET_PG_IDX_DDL:
					result = mapper.getPgIdxDdl(work.params);
					break;
				}
			}
			shutdown();
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
	}
	
	@Override
	public void shutdown() {
		if(!stop) stop = true;
		sqlSession.close();
	}
	
	public Object getResult() {
		return result;
	}
	
	public String getPoolName() {
		return poolName;
	}
	
	public enum WORK_TYPE {
		GET_TABLE_NAME, GET_SOURCE_TABLE_DATA, GET_CREATE_TABLE, GET_TABLE_INFORM
		, GET_COLUMN_INFORM, GET_CONSTRAINT_INFORM, GET_KEY_INFORM, GET_AUTOINCREMENT_INFORM
		, GET_PG_CURRENT_SCHEMA, GET_PG_IDX_DDL, GET_PG_FK_DDL
	}
	
	
}
