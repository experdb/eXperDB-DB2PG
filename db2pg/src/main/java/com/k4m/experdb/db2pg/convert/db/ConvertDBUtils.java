package com.k4m.experdb.db2pg.convert.db;

import java.util.ArrayList;
import java.util.List;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.table.View;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;

public class ConvertDBUtils {
	/** [Add] sequence extract function **/
	/**
	 * <br>
	 * schema's name : schema_name (string) <br>
	 * table's name : table_name (string) <br>
	 * table has comment : table_comment (string)
	 */
	public static List<Table> getTableInform(List<String> tableNames, boolean tableOnly, String srcPoolName,
			DBConfigInfo dbConfigInfo) {
		List<Table> tables = new ArrayList<Table>();
		try {
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				tables = OracleConvertDBUtils.getTableInform(tableNames, tableOnly, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				tables = TiberoConvertDBUtils.getTableInform(tableNames, tableOnly, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				tables = MsSQLConvertDBUtils.getTableInform(tableNames, tableOnly, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				tables = MySQLConvertDBUtils.getTableInform(tableNames, tableOnly, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				tables = AltibaseConvertDBUtils.getTableInform(tableNames, tableOnly, srcPoolName, dbConfigInfo);
			} else {
				

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
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
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				table = OracleConvertDBUtils.setColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				table = TiberoConvertDBUtils.setColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				table = MsSQLConvertDBUtils.setColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				table = MySQLConvertDBUtils.setColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				table = AltibaseConvertDBUtils.setColumnInform(table, srcPoolName, dbConfigInfo);
			} else {

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
		} 
		return table;
	}

	public static Table setPartitionTableColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				table = OracleConvertDBUtils.setPartitionTableColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				
			} else {

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
			System.out.println(e);
		} 
		return table;
	}
	
	public static Table setSubPartitionTableColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				table = OracleConvertDBUtils.setSubPartitionTableColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				
			} else {

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
			System.out.println(e);
		} 
		return table;
	}

	
	public static Table checkColumnInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				table = OracleConvertDBUtils.checkColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				
			} else {

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
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
	 * &nbsp - U : unique key <br>
	 * &nbsp - F : foreign key <br>
	 * Index's type : index_type (enum) <br>
	 * &nbsp - HASH : hash index <br>
	 * &nbsp - BTREE : btree index <br>
	 * Match option of the foreign key constraint : match_option (enum) <br>
	 * &nbsp - FULL : MATCH FULL <br>
	 * &nbsp - PARTIAL : PostgreSQL is not yet implemented. ( 2018.07.30 ) <br>
	 * &nbsp - SIMPLE : MATCH SIMPLE <br>
	 * ON UPDATE clause specifies the action to perform when a referenced column
	 * in the referenced table is being updated to a new value : update_rule
	 * (enum) <br>
	 * &nbsp - NO ACTION <br>
	 * &nbsp - RESTRICT <br>
	 * &nbsp - CASCADE <br>
	 * &nbsp - SET NULL <br>
	 * &nbsp - SET DEFAULT <br>
	 * ON DELETE clause specifies the action to perform when a referenced row in
	 * the referenced table is being deleted : delete_rule (enum) <br>
	 * &nbsp - NO ACTION <br>
	 * &nbsp - RESTRICT <br>
	 * &nbsp - CASCADE <br>
	 * &nbsp - SET NULL <br>
	 * &nbsp - SET DEFAULT
	 */
	public static Table setConstraintInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				table = OracleConvertDBUtils.setConstraintInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				table = TiberoConvertDBUtils.setConstraintInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				table = MsSQLConvertDBUtils.setConstraintInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				table = MySQLConvertDBUtils.setConstraintInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				table = AltibaseConvertDBUtils.setConstraintInform(table, srcPoolName, dbConfigInfo);
			} else {

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
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
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				table = OracleConvertDBUtils.setKeyInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				table = TiberoConvertDBUtils.setKeyInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				table = MsSQLConvertDBUtils.setKeyInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				table = MySQLConvertDBUtils.setKeyInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				table = AltibaseConvertDBUtils.setKeyInform(table, srcPoolName, dbConfigInfo);
			} else {
				
			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
		} 
		return table;
	}

	
	public static  List<View> setViewInform(String tableSchema, String srcPoolName, DBConfigInfo dbConfigInfo) {
		List<View> views = new ArrayList<View>();
		try {
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				views = OracleConvertDBUtils.setViewInform(tableSchema, srcPoolName, dbConfigInfo);
			}else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				views = TiberoConvertDBUtils.setViewInform(tableSchema, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
				views = MsSQLConvertDBUtils.setViewInform(tableSchema, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				views = MySQLConvertDBUtils.setViewInform(tableSchema, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				//views = AltibaseConvertDBUtils.setViewInform(tableSchema, srcPoolName, dbConfigInfo);
			} else {

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
		} 
		return views;
	}
	
	public static Table setSequencesInform(Table table, String srcPoolName, DBConfigInfo dbConfigInfo) {
		try {
			String dbtype = dbConfigInfo.DB_TYPE;
			if (dbtype.equals(Constant.DB_TYPE.ORA)) {
				table = OracleConvertDBUtils.setSequencesInform(table, srcPoolName, dbConfigInfo);
			}else if (dbtype.equals(Constant.DB_TYPE.TBR)) {
				table = TiberoConvertDBUtils.setSequencesInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MSS)) {
//				table = MsSQLConvertDBUtils.setColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.MYS)) {
				table = MySQLConvertDBUtils.setColumnInform(table, srcPoolName, dbConfigInfo);
			} else if (dbtype.equals(Constant.DB_TYPE.ALT)) {
				table = AltibaseConvertDBUtils.setSequencesInform(table, srcPoolName, dbConfigInfo);
			} else {

			}
		} catch (Exception e) {
			LogUtils.error(e.getMessage(), ConvertDBUtils.class);
		} 
		return table;
	}

}
