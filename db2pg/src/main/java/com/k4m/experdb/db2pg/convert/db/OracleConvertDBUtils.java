package com.k4m.experdb.db2pg.convert.db;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Sequence;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.table.View;
import com.k4m.experdb.db2pg.convert.table.key.ForeignKey;
import com.k4m.experdb.db2pg.convert.table.key.Key.IndexType;
import com.k4m.experdb.db2pg.convert.table.key.Key.Type;
import com.k4m.experdb.db2pg.convert.table.key.NormalKey;
import com.k4m.experdb.db2pg.convert.table.key.PrimaryKey;
import com.k4m.experdb.db2pg.convert.table.key.UniqueKey;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyDelete;
import com.k4m.experdb.db2pg.convert.table.key.option.ReferenceDefinition;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.db.impl.WORK_TYPE;

public class OracleConvertDBUtils {
	static MsgCode msgCode = new MsgCode();
	/** [Add] sequence extract function **/
	/**
	 * <br>
	 * schema's name : schema_name (string) <br>
	 * table's name : table_name (string) <br>
	 * table has comment : table_comment (string)
	 */
	public static List<Table> getTableInform(List<String> tableNames, boolean tableOnly, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<Table> tables = new ArrayList<Table>();
		try {
			LogUtils.info(msgCode.getCode("C0031"), OracleConvertDBUtils.class);
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("TABLE_SCHEMA", dbConfigInfo.SCHEMA_NAME);

			if (tableNames != null && !tableNames.isEmpty()) {
				StringBuilder sb = new StringBuilder();
				for (String tableName : tableNames) {
					sb.append("'");
					sb.append(tableName);
					sb.append("',");
				}
				sb.deleteCharAt(sb.length() - 1);
				params.put("TABLE_LIST", sb.toString());
			} else {
				params.put("TABLE_LIST", "");
			}
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_TABLE_INFORM, params));
			mew.run();
			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			//LogUtils.info(msgCode.getCode("C0032") + results, OracleConvertDBUtils.class);
			//LogUtils.info(msgCode.getCode("C0033") + results, OracleConvertDBUtils.class);
			Object obj = null;
			for (Map<String, Object> result : results) {
				Table table = new Table();
				obj = result.get("schema_name");
				if (obj != null) table.setSchemaName(obj.toString());
				obj = result.get("table_name");
				if (obj != null) table.setName(obj.toString());
				obj = result.get("table_comment");
				if (obj != null) table.setComment(obj.toString());
				
				// Oracle Partition Table
				obj = result.get("pt_cnt");
				if(obj != null) table.setPtCnt(Integer.valueOf(obj.toString()));
				obj = result.get("pt_sub_cnt");
				if(obj != null) table.setPtSubCnt(Integer.valueOf(obj.toString()));
				obj = result.get("pt_type");
				if (obj != null) table.setPtType(obj.toString());
				obj = result.get("pt_sub_type");
				if (obj != null) table.setPtSubType(obj.toString());
				obj = result.get("pt_key_column");
				if(obj != null) table.setPartKeyColumn(obj.toString());
				obj = result.get("pt_sub_key_column");
				if(obj != null) table.setPartSubKeyColumn(obj.toString());
				
				tables.add(table);
			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
		} finally {
			LogUtils.info(msgCode.getCode("C0034"), OracleConvertDBUtils.class);
		}
		return tables;
	}

	/**
	 * <br>
	 * Column's ordinal position : ordinal_position (int) <br>
	 * Column's name : column_name (string) <br>
	 * Column's type : column_type (string) <br>
	 * Column has default value : column_default (string) <br>
	 * Column has contain null : is_null (boolean) <br>
	 * Column has comment : column_comment (string) <br>
	 * Any additional information that is available about a given column : extra
	 * (string) <br>
	 * Precision setting when the column data type is numeric :
	 * numeric_precision (int) <br>
	 * Scale setting when the column data type is numeric : numeric_scale (int)
	 * <br>
	 * Sequnce's start value : seq_start (long) <br>
	 * Sequnce's minimal value : seq_min_value (long) <br>
	 * Sequnce's increment value : seq_inc_value (long)
	 */
	public static Table setColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info(msgCode.getCode("C0035"), OracleConvertDBUtils.class);
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("TABLE_SCHEMA", table.getSchemaName());
			params.put("TABLE_NAME", table.getName());
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_COLUMN_INFORM, params));
			mew.run();
			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			//LogUtils.info(msgCode.getCode("C0036") + results, OracleConvertDBUtils.class);
			Object obj = null;
			for (Map<String, Object> result : results) {
				Column column = new Column();
				obj = result.get("ordinal_position");
				if (obj != null) column.setOrdinalPosition(Integer.valueOf(obj.toString()));
				obj = result.get("column_name");
				column.setName(obj != null ? obj.toString() : null);
				obj = result.get("column_default");
				column.setDefaultValue(obj != null ? obj.toString() : null);
				obj = result.get("type_length");
        		if(obj != null) column.setTypeLength(Integer.valueOf(obj.toString()));
				obj = result.get("is_null");
				if (obj != null) column.setNotNull(!Boolean.valueOf(obj.toString()));
				obj = result.get("numeric_precision");
				if (obj != null) column.setNumericPrecision(Integer.valueOf(obj.toString()));
				obj = result.get("numeric_scale");
				if (obj != null) column.setNumericScale(Long.valueOf(obj.toString()));
				obj = result.get("column_type");
				column.setType(obj != null ? obj.toString() : null);
				obj = result.get("column_comment");
				column.setComment(obj != null ? obj.toString() : null);
				obj = result.get("seq_start");
				if (obj != null) column.setSeqStart(Long.valueOf(obj.toString()));
				obj = result.get("seq_min_value");
				if (obj != null) column.setSeqMinValue(Long.valueOf(obj.toString()));
				obj = result.get("seq_inc_value");
				if (obj != null) column.setSeqIncValue(Long.valueOf(obj.toString()));
				obj = result.get("extra");
				column.setExtra(obj != null ? obj.toString() : null);
				table.getColumns().add(column);
			}
			Collections.sort(table.getColumns(), Column.getComparator());
			LogUtils.info(msgCode.getCode("C0037"), OracleConvertDBUtils.class);
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
		}
		return table;
	}
	// Partition Table Column Inform
	public static Table setPartitionTableColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("TABLE_SCHEMA", table.getSchemaName());
			params.put("TABLE_NAME", table.getName());
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_PARTITION_TABLE_COLUMN_INFORM, params));
			mew.run();
			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			Object obj = null;
			for (Map<String, Object> result : results) {
				Column column = new Column();
				obj = result.get("partition_position");
				if (obj != null)
					column.setPartitionPosition(Integer.valueOf(obj.toString()));
				obj = result.get("partition_name");
				column.setPartitionName(obj != null ? obj.toString() : null);
				obj = result.get("high_value");
				column.setHighValue(obj != null ? obj.toString() : null);
				obj = result.get("partitioning_type");
				column.setPartitioningType(obj != null ? obj.toString() : null);
				obj = result.get("partition_column_name");
				column.setPartitionColumnName(obj != null ? obj.toString() : null);
				obj = result.get("partition_column_position");
        		if(obj != null) column.setPartitionColumnPosition(Integer.valueOf(obj.toString()));
        		column.setPartitionTableName(table.getName());
        		obj = result.get("data_type");
        		column.setType(obj != null ? obj.toString() : null);
        		//System.out.println("DATA_TYPE:"+result.get("data_type").toString()+":"+column.getPartitionPosition());
        		if(column.getType().equals("DATE") && !column.getHighValue().equals("MAXVALUE") && !column.getHighValue().equals("MINVALUE")) {
        			params.put("value", column.getHighValue());
        			MetaExtractWorker mew2 = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_FROM_DUAL, params));
        			mew2.run();
        			String high_value = (String)mew2.getResult();
        			column.setHighValue(high_value);
        			//System.out.println("HIGH_VALUE:"+result.get("high_value").toString()+":"+high_value);
        		}

				table.getPartColumns().add(column);
			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
			System.out.println(e);
		}
		return table;
	}
	
	// Sub Partition Table Column Inform
	public static Table setSubPartitionTableColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("TABLE_SCHEMA", table.getSchemaName());
			params.put("TABLE_NAME", table.getName());
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_SUBPARTITION_TABLE_COLUMN_INFORM, params));
			mew.run();
			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			Object obj = null;
			for (Map<String, Object> result : results) {
				Column column = new Column();
				obj = result.get("subpartition_position");
				if (obj != null) column.setSubPartitionPosition(Integer.valueOf(obj.toString()));
				obj = result.get("subpartition_name");
				column.setSubPartitionName(obj != null ? obj.toString() : null);
				obj = result.get("high_value");
				column.setHighValue(obj != null ? obj.toString() : null);
				obj = result.get("subpartitioning_type");
				column.setSubPartitioningType(obj != null ? obj.toString() : null);
				obj = result.get("partition_column_name");
				column.setPartitionColumnName(obj != null ? obj.toString() : null);
				obj = result.get("partition_column_position");
        		if(obj != null) column.setPartitionColumnPosition(Integer.valueOf(obj.toString()));
        		column.setPartitionTableName(result.get("partition_name").toString());
        		obj = result.get("data_type");
        		column.setType(obj != null ? obj.toString() : null);
        		//System.out.println("DATA_TYPE:"+result.get("data_type").toString()+":"+column.getPartitionPosition());
        		if(column.getType().equals("DATE") && !column.getHighValue().equals("MAXVALUE") && !column.getHighValue().equals("MINVALUE")) {
        			params.put("value", column.getHighValue());
        			MetaExtractWorker mew2 = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_FROM_DUAL, params));
        			mew2.run();
        			String high_value = (String)mew2.getResult();
        			column.setHighValue(high_value);
        			//System.out.println("HIGH_VALUE:"+result.get("high_value").toString()+":"+high_value);
        		}
        		
				table.getSubPartColumns().add(column);
			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
			System.out.println(e);
		}
		return table;
	}
	
	// XMLTYPE Check
	public static Table checkColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			Boolean checkColumn = false;
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("TABLE_SCHEMA", table.getSchemaName());
			params.put("TABLE_NAME", table.getName());
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_COLUMN_INFORM, params));
			mew.run();

			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			Object obj = null;
			for (Map<String, Object> result : results) {
				Column column = new Column();
				
				if(((String)result.get("column_type")).contains("XMLTYPE")) checkColumn=true;

				obj = result.get("column_name");
				column.setName(obj != null ? obj.toString() : null);

				obj = result.get("column_type");
				column.setType(obj != null ? obj.toString() : null);

				table.getColumns().add(column);
			}
			table.setCheckColumn(checkColumn);
			//Collections.sort(table.getColumns(), Column.getComparator());
			
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
		}
		return table;
	}

	/**
	 * <br>
	 * Constraint's schema name : constraint_schema (string) <br>
	 * Constraint's name : constraint_name (string) <br>
	 * Table's schema name : table_schema (string) <br>
	 * Table's name : table_name (string) <br>
	 * Column's name : column_name (string) <br>
	 * Column's ordinal position : ordinal_position (int) <br>
	 * Referenced table's schema name : ref_table_schema (string) <br>
	 * Referenced table's name : ref_table_name (string) <br>
	 * Referenced Column's name : ref_column_name (string) <br>
	 * Constraint's type : constraint_type (enum) <br>
	 * &nbsp - P : primary key <br>
	 * &nbsp - U : unique key <br>---------
	 * &nbsp - F : foreign key <br>
	 * 
	 * C - Check constraint on a table
	 * 
	 * Index's type : index_type (enum) <br>
	 * &nbsp - NORMAL :   <br>
	 * &nbsp - BITMAP :   <br>
	 * &nbsp - FUNCTION-BASED NORMAL :   <br>
	 * &nbsp - FUNCTION-BASED BITMAP :   <br>
	 * &nbsp - DOMAIN :   <br>
	 * 
	 * ON DELETE clause specifies the action to perform when a referenced row in
	 * the referenced table is being deleted : delete_rule (enum) <br>
	 * &nbsp - NO ACTION <br>
	 * &nbsp - CASCADE <br>
	 * 
	 * EFERRABLE Indicates whether the constraint is deferrable (DEFERRABLE) or not (NOT DEFERRABLE) 
	 * &nbsp - DEFERRABLE
	 * &nbsp - NOT DEFERRABLE
	 * 
	 * DEFERRED Indicates whether the constraint was initially deferred (DEFERRED) or not (IMMEDIATE)
	 * &nbsp - DEFERRED
	 * &nbsp - IMMEDIATE
	 */
	public static Table setConstraintInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info(msgCode.getCode("C0038"), OracleConvertDBUtils.class);
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("TABLE_SCHEMA", table.getSchemaName());	
			params.put("TABLE_NAME", table.getName());
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_CONSTRAINT_INFORM, params));
			mew.run();
			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			//LogUtils.info(msgCode.getCode("C0039") + results, ConvertDBUtils.class);
			Object obj = null;
			for (Map<String, Object> result : results) {
				String constraintType = (obj = result.get("constraint_type")) != null ? obj.toString() : null;
				if (constraintType.equals("P")) {
					String keySchema = (obj = result.get("constraint_schema")) != null ? obj.toString() : null;
					String keyName = (obj = result.get("constraint_name")) != null ? obj.toString() : null;
					String tableSchema = (obj = result.get("table_schema")) != null ? obj.toString() : null;
					String tableName = (obj = result.get("table_name")) != null ? obj.toString() : null;
					String columnName = (obj = result.get("column_name")) != null ? obj.toString() : null;
					obj = result.get("ordinal_position");
					int ordinalPosition = -1;
					if (obj != null)
						ordinalPosition = Integer.valueOf(obj.toString());
					String indexType = (obj = result.get("index_type")) != null ? obj.toString() : null;
					String indexName = (obj = result.get("index_name")) != null ? obj.toString() : null;
					PrimaryKey pkey = new PrimaryKey();
					boolean isAdded = false;
					for (int i = 0; i < table.getKeys().size(); i++) {
						if (table.getKeys().get(i).getType().name().equals(Type.PRIMARY.name())) {
							if (table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
								table.getKeys().get(i).getColumns().add(columnName);
								table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
								columnName = null;
								ordinalPosition = -1;
								isAdded = true;
								break;
							}
						}
					}
					if (isAdded)
						continue;

					if (pkey.getColumns() == null)
						pkey.setColumns(new ArrayList<String>());
					if (columnName != null) {
						pkey.getColumns().add(columnName);
					}
					if (pkey.getOrdinalPositions() == null)
						pkey.setOrdinalPositions(new ArrayList<Integer>());
					if (ordinalPosition != -1) {
						pkey.getOrdinalPositions().add(ordinalPosition);
					}
					pkey.setTableSchema(tableSchema);
					pkey.setTableName(tableName);
					pkey.setKeySchema(keySchema);
					pkey.setName(keyName);
					pkey.setIndexName(indexName);
					
					if (indexType == null) {
						pkey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("NORMAL")) {
						pkey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("BITMAP")) {
						pkey.setIndexType(IndexType.BITMAP);
					} else if (indexType.equals("FUNCTION_BASED_NORMAL")) {
						pkey.setIndexType(IndexType.FUNCTION_BASED_NORMAL);
					} else if (indexType.equals("FUNCTION_BASED_BITMAP")) {
						pkey.setIndexType(IndexType.FUNCTION_BASED_BITMAP);
					} else if (indexType.equals("DOMAIN")) {
						pkey.setIndexType(IndexType.DOMAIN);
					}
					
					table.getKeys().add(pkey);
				} else if (constraintType.equals("U")) {
					String keySchema = (obj = result.get("constraint_schema")) != null ? obj.toString() : null;
					String keyName = (obj = result.get("constraint_name")) != null ? obj.toString() : null;
					String tableSchema = (obj = result.get("table_schema")) != null ? obj.toString() : null;
					String tableName = (obj = result.get("table_name")) != null ? obj.toString() : null;
					String columnName = (obj = result.get("column_name")) != null ? obj.toString() : null;
					obj = result.get("ordinal_position");
					int ordinalPosition = -1;
					if (obj != null)
						ordinalPosition = Integer.valueOf(obj.toString());
					String indexType = (obj = result.get("index_type")) != null ? obj.toString() : null;

					UniqueKey ukey = new UniqueKey();
					boolean isAdded = false;
					for (int i = 0; i < table.getKeys().size(); i++) {
						if (table.getKeys().get(i).getType().name().equals(Type.UNIQUE.name())) {
							if (table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
								table.getKeys().get(i).getColumns().add(columnName);
								table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
								columnName = null;
								ordinalPosition = -1;
								isAdded = true;
								break;
							}
						}
					}
					if (isAdded)
						continue;

					if (ukey.getColumns() == null)
						ukey.setColumns(new ArrayList<String>());
					if (columnName != null) {
						ukey.getColumns().add(columnName);
					}
					if (ukey.getOrdinalPositions() == null)
						ukey.setOrdinalPositions(new ArrayList<Integer>());
					if (ordinalPosition != -1) {
						ukey.getOrdinalPositions().add(ordinalPosition);
					}
					ukey.setTableSchema(tableSchema);
					ukey.setTableName(tableName);
					ukey.setKeySchema(keySchema);
					ukey.setName(keyName);
					if (indexType == null) {
						ukey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("NORMAL")) {
						ukey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("BITMAP")) {
						ukey.setIndexType(IndexType.BITMAP);
					} else if (indexType.equals("FUNCTION_BASED_NORMAL")) {
						ukey.setIndexType(IndexType.FUNCTION_BASED_NORMAL);
					} else if (indexType.equals("FUNCTION_BASED_BITMAP")) {
						ukey.setIndexType(IndexType.FUNCTION_BASED_BITMAP);
					} else if (indexType.equals("DOMAIN")) {
						ukey.setIndexType(IndexType.DOMAIN);
					}
					table.getKeys().add(ukey);
				} else if (constraintType.equals("F")) {
					String keySchema = (obj = result.get("constraint_schema")) != null ? obj.toString() : null;
					String keyName = (obj = result.get("constraint_name")) != null ? obj.toString() : null;
					String tableSchema = (obj = result.get("table_schema")) != null ? obj.toString() : null;
					String tableName = (obj = result.get("table_name")) != null ? obj.toString() : null;
					String columnName = (obj = result.get("ref_column_name")) != null ? obj.toString() : null;
					obj = result.get("ordinal_position");
					int ordinalPosition = -1;
					if (obj != null)
						ordinalPosition = Integer.valueOf(obj.toString());
					String indexType = (obj = result.get("index_type")) != null ? obj.toString() : null;
					String refTableSchema = (obj = result.get("ref_table_schema")) != null ? obj.toString() : null;
					String refTable = (obj = result.get("ref_table_name")) != null ? obj.toString() : null;
					String refColumnName = (obj = result.get("ref_column_name")) != null ? obj.toString() : null;
					String deferrable = (obj = result.get("deferrable")) != null ? obj.toString() : null;
					String deferred = (obj = result.get("deferred")) != null ? obj.toString() : null;
					String deleteRule = (obj = result.get("delete_rule")) != null ? obj.toString() : null;

					ForeignKey fkey = new ForeignKey();

					boolean isAdded = false;
					
					for (int i = 0; i < table.getKeys().size(); i++) {
						if (table.getKeys().get(i).getType().name().equals(Type.FOREIGN.name())) {
							ForeignKey key = (ForeignKey) table.getKeys().get(i);
							if (key.isSameKey(tableSchema, tableName, keySchema, keyName, refTableSchema, refTable)) {
								if (fkey.getColumns() == null)
									fkey.setColumns(new ArrayList<String>());
								key.getColumns().add(columnName);
								if (fkey.getOrdinalPositions() == null)
									fkey.setOrdinalPositions(new ArrayList<Integer>());
								key.getOrdinalPositions().add(ordinalPosition);
								if (fkey.getRefColumns() == null)
									fkey.setRefColumns(new ArrayList<String>());
								key.getRefColumns().add(refColumnName);
								columnName = null;
								ordinalPosition = -1;
								isAdded = true;
								break;
							}
						}
					}
					if (isAdded)
						continue;

					if (fkey.getColumns() == null)
						fkey.setColumns(new ArrayList<String>());
					if (columnName != null) {
						fkey.getColumns().add(columnName);
					}
					if (fkey.getRefColumns() == null)
						fkey.setRefColumns(new ArrayList<String>());
					if (refColumnName != null) {
						fkey.getRefColumns().add(refColumnName);
					}
					if (fkey.getOrdinalPositions() == null)
						fkey.setOrdinalPositions(new ArrayList<Integer>());
					if (ordinalPosition != -1) {
						fkey.getOrdinalPositions().add(ordinalPosition);
					}

					if (fkey.getRefDef() == null) {
						fkey.setRefDef(new ReferenceDefinition());
					}

					if (deleteRule != null) {
						if (deleteRule.toUpperCase().contains("CASCADE")) {
							fkey.getRefDef().setDelete(ForeignKeyDelete.CASCADE);
						} else if (deleteRule.toUpperCase().contains("NO")
								&& deleteRule.toUpperCase().contains("ACTION")) {
							fkey.getRefDef().setDelete(ForeignKeyDelete.NO_ACTION);
						} 
					}
					
					fkey.setTableSchema(tableSchema);
					fkey.setTableName(tableName);
					fkey.setKeySchema(keySchema);
					fkey.setName(keyName);
					fkey.setRefTableSchema(refTableSchema);
					fkey.setRefTable(refTable);
					fkey.setDeferrable(deferrable);
					fkey.setDeferred(deferred);
					if (indexType == null) {
						fkey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("NORMAL")) {
						fkey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("BITMAP")) {
						fkey.setIndexType(IndexType.BITMAP);
					} else if (indexType.equals("FUNCTION_BASED_NORMAL")) {
						fkey.setIndexType(IndexType.FUNCTION_BASED_NORMAL);
					} else if (indexType.equals("FUNCTION_BASED_BITMAP")) {
						fkey.setIndexType(IndexType.FUNCTION_BASED_BITMAP);
					} else if (indexType.equals("DOMAIN")) {
						fkey.setIndexType(IndexType.DOMAIN);
					}
					
					table.getKeys().add(fkey);
				}

			}
			Collections.sort(table.getColumns(), Column.getComparator());
			LogUtils.info(msgCode.getCode("C0040"), OracleConvertDBUtils.class);
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
		}
		return table;
	}

	/**
	 * <br>
	 * Index's schema name : index_schema (string) <br>
	 * Index's name : index_name (string) <br>
	 * Table's schema name : table_schema (string) <br>
	 * Table's name : table_name (string) <br>
	 * Column's name : column_name (string) <br>
	 * Column's index ordinal position : ordinal_position (int) <br>
	 * Index's type : index_type (enum) <br>
	 * &nbsp - HASH : hash index <br>
	 * &nbsp - BTREE : btree index
	 */
	public static Table setKeyInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info(msgCode.getCode("C0041"), OracleConvertDBUtils.class);
			Map<String, Object> params = new HashMap<String, Object>();

			params.put("TABLE_SCHEMA", table.getSchemaName());
			params.put("TABLE_NAME", table.getName());

			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_KEY_INFORM, params));
			mew.run();
			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			//LogUtils.info(msgCode.getCode("C0042") + results, ConvertDBUtils.class);
			Object obj = null;
			for (Map<String, Object> result : results) {
				obj = result.get("index_schema");
				String keySchema = obj != null ? obj.toString() : null;
				obj = result.get("index_name");
				String keyName = obj != null ? obj.toString() : null;
				obj = result.get("table_schema");
				String tableSchema = obj != null ? obj.toString() : null;
				obj = result.get("table_name");
				String tableName = obj != null ? obj.toString() : null;
				obj = result.get("column_name");
				String columnName = obj != null ? obj.toString() : null;
				obj = result.get("ordinal_position");
				int ordinalPosition = -1;
				if (obj != null)
					ordinalPosition = Integer.valueOf(obj.toString());
				obj = result.get("index_type");
				String indexType = obj != null ? obj.toString() : null;
				obj = result.get("uniqueness");
				String uniqueness = obj != null ? obj.toString() : null;
				
				NormalKey nkey = new NormalKey();
				UniqueKey ukey = new UniqueKey();
				boolean isAdded = false;

				if(uniqueness.equals("UNIQUE")){
					for (int i = 0; i < table.getKeys().size(); i++) {
						if (table.getKeys().get(i).getType().name().equals(Type.UNIQUE.name())) {
							if (table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
								table.getKeys().get(i).getColumns().add(columnName);
								table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
								columnName = null;
								ordinalPosition = -1;
								isAdded = true;
								break;
							}
						}
					}
					if (isAdded)
						continue;

					if (ukey.getColumns() == null)
						ukey.setColumns(new ArrayList<String>());
					if (columnName != null) {
						ukey.getColumns().add(columnName);
					}
					if (ukey.getOrdinalPositions() == null)
						ukey.setOrdinalPositions(new ArrayList<Integer>());
					if (ordinalPosition != -1) {
						ukey.getOrdinalPositions().add(ordinalPosition);
					}
					ukey.setTableSchema(tableSchema);
					ukey.setTableName(tableName);
					ukey.setKeySchema(keySchema);
					ukey.setName(keyName);
					if (indexType == null) {
						ukey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("NORMAL")) {
						ukey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("BITMAP")) {
						ukey.setIndexType(IndexType.BITMAP);
					} else if (indexType.equals("FUNCTION_BASED_NORMAL")) {
						ukey.setIndexType(IndexType.FUNCTION_BASED_NORMAL);
					} else if (indexType.equals("FUNCTION_BASED_BITMAP")) {
						ukey.setIndexType(IndexType.FUNCTION_BASED_BITMAP);
					} else if (indexType.equals("DOMAIN")) {
						ukey.setIndexType(IndexType.DOMAIN);
					}
					table.getKeys().add(ukey);
					
				}else if(uniqueness.equals("NONUNIQUE")){
					for (int i = 0; i < table.getKeys().size(); i++) {
						if (table.getKeys().get(i).getType().name().equals(Type.NORMAL.name())) {
							if (table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
								table.getKeys().get(i).getColumns().add(columnName);
								table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
								columnName = null;
								ordinalPosition = -1;
								isAdded = true;
								break;
							}
						}
					}
					if (isAdded)
						continue;
					if (nkey.getColumns() == null)
						nkey.setColumns(new ArrayList<String>());
					if (columnName != null) {
						nkey.getColumns().add(columnName);
					}
					if (nkey.getOrdinalPositions() == null)
						nkey.setOrdinalPositions(new ArrayList<Integer>());
					if (ordinalPosition != -1) {
						nkey.getOrdinalPositions().add(ordinalPosition);
					}
					nkey.setTableSchema(tableSchema);
					nkey.setTableName(tableName);
					nkey.setKeySchema(keySchema);
					nkey.setName(keyName);
					if (indexType == null) {
						nkey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("NORMAL")) {
						nkey.setIndexType(IndexType.NORMAL);
					} else if (indexType.equals("BITMAP")) {
						nkey.setIndexType(IndexType.BITMAP);
					} else if (indexType.equals("FUNCTION_BASED_NORMAL")) {
						nkey.setIndexType(IndexType.FUNCTION_BASED_NORMAL);
					} else if (indexType.equals("FUNCTION_BASED_BITMAP")) {
						nkey.setIndexType(IndexType.FUNCTION_BASED_BITMAP);
					} else if (indexType.equals("DOMAIN")) {
						nkey.setIndexType(IndexType.DOMAIN);
					}
					
					table.getKeys().add(nkey);
				}
				
			}

			Collections.sort(table.getColumns(), Column.getComparator());
			LogUtils.info(msgCode.getCode("C0043"), OracleConvertDBUtils.class);
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
		}
		return table;
	}
	
	public static List<View> setViewInform(String Schema, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<View> views = new ArrayList<View>();
		try {
			LogUtils.info(msgCode.getCode("C0044"),OracleConvertDBUtils.class);
			Map<String,Object> params = new HashMap<String,Object>();
			
//			params.put("TABLE_SCHEMA", Schema);
			
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_VIEW_INFORM, params));
			mew.run();
			List<Map<String,Object>> results = (List<Map<String,Object>>)mew.getListResult();
			LogUtils.info(msgCode.getCode("C0045")+results,OracleConvertDBUtils.class);
			
			Object obj = null;
        	for (Map<String,Object> result : results) {
        		obj = result.get("view_name");
    			String viewName = obj!=null?obj.toString():null;
    			obj = result.get("text");
    			String text = obj!=null?obj.toString():null;
    			obj = result.get("view_definition");
    			String viewDefinition="create view "+viewName.toLowerCase()+" as "+ text.toLowerCase()+";";
    			View view = new View();
    			boolean isAdded = false;
    			/*for(int i=0; i<table.getKeys().size();i++) {
    				if(table.getKeys().get(i).getType().name().equals(Type.VIEW.name())) {
    					if(table.getKeys().get(i).isSameKey(tableSchema, tableName, keySchema, keyName)) {
    						table.getKeys().get(i).getColumns().add(columnName);
    						table.getKeys().get(i).getOrdinalPositions().add(ordinalPosition);
    						columnName = null;
    						ordinalPosition = -1;
    						isAdded = true;
    						break;
    					}
    				}
    			}*/
//    			view.setTableCatalLog(tableCatalLog);
//    			view.setTableSchema(tableSchema);
//    			view.setTableName(tableName);
    			view.setViewDefinition(viewDefinition);
//    			view.setCheckOption(checkOption);
//    			view.setIsUpdaTable(isUpdaTable);
    			
    			views.add(view);
        	}       	
			LogUtils.info(msgCode.getCode("C0046"),OracleConvertDBUtils.class);
		} catch(Exception e){
			LogUtils.error(e.getMessage(),OracleConvertDBUtils.class);
		}
		return views;
	}
	
	public static Table setSequencesInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			LogUtils.info(msgCode.getCode("C0047"), OracleConvertDBUtils.class);
			Map<String, Object> params = new HashMap<String, Object>();
			MetaExtractWorker mew = new MetaExtractWorker(srcPoolName, new MetaExtractWork(WORK_TYPE.GET_SEQUENCE_INFORM, params));
			mew.run();
			List<Map<String, Object>> results = (List<Map<String, Object>>) mew.getListResult();
			//LogUtils.info(msgCode.getCode("C0048") + results, OracleConvertDBUtils.class);
			Object obj = null;
			for (Map<String, Object> result : results) {
				Sequence sequence = new Sequence();
				obj = result.get("seq_start");
				if (obj != null)
					sequence.setSeqStart(Long.valueOf(obj.toString()));
				obj = result.get("seq_min_value");
				if (obj != null)
					sequence.setSeqMinValue(Long.valueOf(obj.toString()));
				obj = result.get("seq_max_value");
				if (obj != null)
					sequence.setSeqMaxalue(new BigDecimal(obj.toString()));
				obj = result.get("seq_inc_value");
				if (obj != null)
					sequence.setSeqIncValue(Long.valueOf(obj.toString()));
				obj = result.get("extra");
				sequence.setSeqName(obj != null ? obj.toString() : null);
				table.getSequence().add(sequence);
			}
			Collections.sort(table.getColumns(), Column.getComparator());
			LogUtils.info(msgCode.getCode("C0049"), OracleConvertDBUtils.class);
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), OracleConvertDBUtils.class);
		}
		return table;
	}
}
