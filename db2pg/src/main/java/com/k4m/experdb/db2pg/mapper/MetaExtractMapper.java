package com.k4m.experdb.db2pg.mapper;

import java.util.List;
import java.util.Map;

public interface MetaExtractMapper {
	public List<?> getTableName(Map<String,Object> params);
	public List<?> getSourceTableData(Map<String,Object> params);
	public List<?> getCreateTable(Map<String,Object> params);
	public List<?> getTableInform(Map<String,Object> params);
	public List<?> getColumnInform(Map<String,Object> params);
	public List<?> getConstraintInform(Map<String,Object> params);
	public List<?> getKeyInform(Map<String,Object> params);
	public List<?> getAutoincrementInform(Map<String,Object> params);
	public List<?> getPgFkDdl(Map<String,Object> params);
	public List<?> getPgIdxDdl(Map<String,Object> params);
	public Map<?,?> getPgCurrentSchema(Map<String,Object> params);
}
