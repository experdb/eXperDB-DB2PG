package com.k4m.experdb.db2pg.work.db.impl;

import java.util.Map;

import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker.WORK_TYPE;

public class MetaExtractWork {
	WORK_TYPE type;
	Map<String,Object> params;
	
	public MetaExtractWork(WORK_TYPE workType) {
		this.type = workType;
	}
	
	public MetaExtractWork(WORK_TYPE workType, Map<String,Object> params) {
		this.params = params;
		this.type = workType;
	}
}
