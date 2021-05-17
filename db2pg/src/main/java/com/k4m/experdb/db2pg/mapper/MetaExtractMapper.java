package com.k4m.experdb.db2pg.mapper;

import java.util.List;
import java.util.Map;

public interface MetaExtractMapper {
	public List<String> getTableNames(Map<String,Object> params);
	public List<?> getSourceTableData(Map<String,Object> params);
	public List<Map<String,Object>> getTableInform(Map<String,Object> params);
	public List<Map<String,Object>> getPartitionTableColumnInform(Map<String,Object> params);
	public List<Map<String,Object>> getSubPartitionTableColumnInform(Map<String,Object> params);
	public List<Map<String,Object>> getColumnInform(Map<String,Object> params);
	public List<Map<String,Object>> getConstraintInform(Map<String,Object> params);
	public List<Map<String,Object>> getKeyInform(Map<String,Object> params);
	public String getFromDual(Map<String,Object> params);
	public List<?> getAutoincrementInform(Map<String,Object> params);
	public List<Map<String,Object>> getPgFkDdl();
	public List<Map<String,Object>> getPgIdxDdl();
	public String getPgCurrentSchema();
	public String getGtype(Map<String,Object> params);
	public String getDimname(Map<String,Object> params);
	public String getSrid(Map<String,Object> params);
	public List<Map<String,Object>> getViewInform(Map<String,Object> params);
	public List<Map<String,Object>> getSequencesInform(Map<String,Object> params);
	public String getPgCharSet(Map<String,Object> params);
	public List<Map<String,Object>> getPgIdxDdlTable(Map<String, Object> param);
	public List<Map<String,Object>> getPgFkDdlTable(Map<String, Object> param);

}
