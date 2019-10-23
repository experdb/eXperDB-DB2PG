package com.k4m.experdb.db2pg.common;

import com.k4m.experdb.db2pg.config.MsgCode;

public class CreateDbStmt {
	static MsgCode msgCode = new MsgCode();

	public static String AddRownumSql(String DB_TYPE, String sql, int rowcnt) throws Exception{
		switch(DB_TYPE){
		case Constant.DB_TYPE.ORA: case Constant.DB_TYPE.TBR:
			sql = "SELECT * FROM ( " + sql + ") T1 WHERE ROWNUM <= " + rowcnt;
			break;
		case Constant.DB_TYPE.POG: case Constant.DB_TYPE.MYS:
			sql = "SELECT * FROM ( " + sql + ") T1 LIMIT " + rowcnt;
			break;
		case Constant.DB_TYPE.MSS: case Constant.DB_TYPE.ASE:
			if (sql.toUpperCase().contains("OPTION")){
				sql = sql.substring(0, 6) + " TOP " +  + rowcnt + sql.substring(6);
			}else{
				sql = "SELECT TOP " + rowcnt + " * FROM ( " + sql + ") T1";
			}
			break;
		case Constant.DB_TYPE.DB2:
			sql = "SELECT * FROM ( " + sql +  ") AS TTT1 FETCH FIRST " + rowcnt + " ROWS ONLY";
			break;
		default :
			throw new Exception(msgCode.getCode("C0001"));
		}
		return sql;
	}
	
	public static String GetTruncateTblDDL(String DB_TYPE, String SCHEMA_NM, String TABLE_NM) {
		String DDL = "";
		
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.MSS : case Constant.DB_TYPE.TBR : 
				DDL = "TRUNCATE TABLE \"" + SCHEMA_NM + "\".\"" + TABLE_NM + "\"";
				break;
			case Constant.DB_TYPE.DB2 :
				DDL = "TRUNCATE TABLE " + SCHEMA_NM + "." + TABLE_NM + " IMMEDIATE";
				break;
			case Constant.DB_TYPE.ASE :
				DDL = "TRUNCATE TABLE " + SCHEMA_NM + "." + TABLE_NM;
			case Constant.DB_TYPE.POG :
				DDL = "TRUNCATE TABLE " + SCHEMA_NM + "." + TABLE_NM;
			default:
				DDL = "TRUNCATE TABLE " + SCHEMA_NM + "." + TABLE_NM;
				break;
		}
		
		return DDL;
	}
	
	public static String GetDisablePrimaryKeyForORA(String DB_TYPE, String SCHEMA_NM, String TABLE_NM) throws Exception{
		String DDL = "";
		
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.POG : case Constant.DB_TYPE.MSS : case Constant.DB_TYPE.TBR : case Constant.DB_TYPE.ASE :
				DDL = "ALTER TABLE " + SCHEMA_NM + "." + TABLE_NM + " DISABLE PRIMARY KEY";				
				break;
			default:
				throw new Exception(msgCode.getCode("C0002"));
		}
		
		return DDL;
	}
	
	public static String GetEnablePrimaryKeyForORA(String DB_TYPE, String SCHEMA_NM, String TABLE_NM, String VALIDATED) throws Exception {
		String DDL = "";
		
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.POG : case Constant.DB_TYPE.MSS : case Constant.DB_TYPE.TBR : case Constant.DB_TYPE.ASE :
				if (VALIDATED.equals("VALIDATED")){
					VALIDATED = "";
				}else{
					VALIDATED = "NOVALIDATE";
				}
				
				DDL = "ALTER TABLE " + SCHEMA_NM + "." + TABLE_NM + " ENABLE " + VALIDATED + " PRIMARY KEY";				
				break;
			default :
				throw new Exception(msgCode.getCode("C0002"));
		}
		
		return DDL;
	}
	
	public static String GetDropIndexDDL(String DB_TYPE, String INDX_OTH_ENTR, String SCHEMA_NM, String TAR_TBL_ENM, String INDX_ENM) throws Exception{
		String DDL = "";
		
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.TBR :
				if (INDX_OTH_ENTR.equals("UC")){
					DDL = "ALTER TABLE " + SCHEMA_NM + "." + TAR_TBL_ENM + " DROP CONSTRAINT " + INDX_ENM;
				}else{
					DDL = "DROP INDEX " + SCHEMA_NM + "." + INDX_ENM;
				}				
				break;
			case Constant.DB_TYPE.DB2 : case Constant.DB_TYPE.POG : case Constant.DB_TYPE.ASE :
				DDL = "DROP INDEX " + SCHEMA_NM + "." + INDX_ENM;			
				break;
			case Constant.DB_TYPE.MSS : 
				break;
			default:
				throw new Exception(msgCode.getCode("C0003"));
		}
		
		return DDL;
	}
	
	public static String getLoggingTblDDL(String DB_TYPE, String SCHEMA_NM, String TAR_TBL_ENM) throws Exception{
		String DDL = "";
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.TBR : 
				DDL = "ALTER TABLE " + SCHEMA_NM + "." + TAR_TBL_ENM + " LOGGING";
				break;
			case Constant.DB_TYPE.POG : case Constant.DB_TYPE.ASE : case Constant.DB_TYPE.MSS : 				
				break;
			case Constant.DB_TYPE.DB2 :
				DDL = "SELECT CURRENT_TIME FROM SYSIBM.SYSDUMMY1";
				break;
			default:
				throw new Exception(msgCode.getCode("C0004"));
		}
		
		return DDL;
	}
	
	public static String getNoLoggingTblDDL(String DB_TYPE, String SCHEMA_NM, String TAR_TBL_ENM) throws Exception{
		String DDL = "";
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.TBR : 
				DDL = "ALTER TABLE " + SCHEMA_NM + "." + TAR_TBL_ENM + " NOLOGGING";
				break;
			case Constant.DB_TYPE.POG : case Constant.DB_TYPE.ASE : case Constant.DB_TYPE.MSS : 				
				break;
			case Constant.DB_TYPE.DB2 :
				DDL = "ALTER TABLE " + SCHEMA_NM + "." + TAR_TBL_ENM + " ACTIVATE NOT LOGGED INITIALLY";
				break;
			default:
				throw new Exception(msgCode.getCode("C0005"));
		}
		
		return DDL;
	}
	
	public static String EnableFkDDL(String DB_TYPE, String SCHEMA_NM, String TAR_TBL_ENM, String CONSTRAINT_NAME) throws Exception{
		String DDL = "";
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.TBR : 
				DDL = "ALTER TABLE " + SCHEMA_NM + "." + TAR_TBL_ENM + "  ENABLE CONSTRAINT " + CONSTRAINT_NAME;
				break;
			case Constant.DB_TYPE.POG : case Constant.DB_TYPE.ASE : case Constant.DB_TYPE.MSS : 				
				break;
			case Constant.DB_TYPE.DB2 :				
				break;
			default:
				throw new Exception(msgCode.getCode("C0006"));
		}
		
		return DDL;
	}
	
	public static String DisableFkDDL(String DB_TYPE, String SCHEMA_NM, String TAR_TBL_ENM, String CONSTRAINT_NAME) throws Exception{
		String DDL = "";
		switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.TBR : 
				DDL = "ALTER TABLE " + SCHEMA_NM + "." + TAR_TBL_ENM + "  DISABLE CONSTRAINT " + CONSTRAINT_NAME;
				break;
			case Constant.DB_TYPE.POG : case Constant.DB_TYPE.ASE : case Constant.DB_TYPE.MSS : 				
				break;
			case Constant.DB_TYPE.DB2 :				
				break;
			default:
				throw new Exception(msgCode.getCode("C0006"));
		}
		
		return DDL;
	}
	
	public static String getDropSequenceDDL(String DB_TYPE, String DB_VER, String SEQUENCE_OWNER, String SEQUENCE_NAME) throws Exception{
		StringBuilder builder = new StringBuilder();
		switch(DB_TYPE){
		case Constant.DB_TYPE.ORA : case Constant.DB_TYPE.TBR : case Constant.DB_TYPE.POG : case Constant.DB_TYPE.ASE : case Constant.DB_TYPE.MSS : case Constant.DB_TYPE.DB2 :	
			builder.append("DROP SEQUENCE \"").append(SEQUENCE_OWNER).append("\".\"").append(SEQUENCE_NAME).append("\"");
			break;
		default:
			throw new Exception(msgCode.getCode("C0007"));
		}
		return builder.toString();
	}
	
	public static String getCreateStatTableDDL(String DB_TYPE, String DB_VER, String SCHEMA_NM, String TABLE_NAME) throws Exception{
		StringBuilder builder = new StringBuilder();
		switch(DB_TYPE){
		case Constant.DB_TYPE.ORA : 
			builder.append("EXEC DBMS_STATS.CREATE_STAT_TABLE(OWNNAME =>'").append(SCHEMA_NM).append("',STATTAB=>'").append(TABLE_NAME).append("')");
			break;
		default:
			throw new Exception(msgCode.getCode("C0008"));
		}
		return builder.toString();
	}
	
	public static String getDropStatTableDDL(String DB_TYPE, String DB_VER, String SCHEMA_NM, String TABLE_NAME) throws Exception{
		StringBuilder builder = new StringBuilder();
		switch(DB_TYPE){
		case Constant.DB_TYPE.ORA : 
			builder.append("EXEC DBMS_STATS.DROP_STAT_TABLE(OWNNAME =>'").append(SCHEMA_NM).append("',STATTAB=>'").append(TABLE_NAME).append("')");
			break;
		default:
			throw new Exception(msgCode.getCode("C0009"));
		}
		return builder.toString();
	}
	
	public static String getCreateSequenceDDL(String DB_TYPE, String DB_VER, String SEQUENCE_OWNER, String SEQUENCE_NAME, String MIN_VALUE, 
											  String MAX_VALUE, int INCREMENT_BY, String CYCLE_FLAG, String ORDER_FLAG, 
											  int CACHE_SIZE, int LAST_NUMBER) {		
		StringBuilder builder = new StringBuilder();
		
		try{
			switch(DB_TYPE){
			case Constant.DB_TYPE.ORA : 
				if (DB_VER.compareTo("9.0") >= 0 || DB_VER.compareTo("10.1") >= 0 ){
					builder.append("CREATE SEQUENCE \"").append(SEQUENCE_OWNER).append("\".\"").append(SEQUENCE_NAME).append("\" INCREMENT BY ").append(INCREMENT_BY);
					builder.append(" START WITH ").append(LAST_NUMBER).append(" MAXVALUE ").append(MAX_VALUE).append(" MINVALUE ").append(MIN_VALUE);
					
					if (CYCLE_FLAG.equals("Y")){
						builder.append(" CYCLE ");
					}else{
						builder.append(" NOCYCLE ");
					}
					
					if (CACHE_SIZE == 0){
						builder.append(" NOCACHE ");
					}else{
						builder.append(" CACHE ").append(CACHE_SIZE);
					}
					
					if (ORDER_FLAG.equals("Y")){
						builder.append(" ORDER ");
					}else{
						builder.append(" NOORDER ");
					}
				}else{
					new Exception(msgCode.getCode("C0010"));
				}

				break;
			case Constant.DB_TYPE.TBR :
				builder.append("CREATE SEQUENCE \"").append(SEQUENCE_OWNER).append("\".\"").append(SEQUENCE_NAME).append("\" INCREMENT BY ").append(INCREMENT_BY);
				builder.append(" START WITH ").append(LAST_NUMBER).append(" MAXVALUE ").append(MAX_VALUE).append(" MINVALUE ").append(MIN_VALUE);
				
				if (CYCLE_FLAG.equals("Y")){
					builder.append(" CYCLE ");
				}else{
					builder.append(" NOCYCLE ");
				}
				
				if (CACHE_SIZE == 0){
					builder.append(" NOCACHE ");
				}else{
					builder.append(" CACHE ").append(CACHE_SIZE);
				}
				
				if (ORDER_FLAG.equals("Y")){
					builder.append(" ORDER ");
				}else{
					builder.append(" NOORDER ");
				}
				break;
			case Constant.DB_TYPE.MSS :	
				if (DB_VER.compareTo("11.0") >= 0){
					builder.append("CREATE SEQUENCE [").append(SEQUENCE_OWNER).append("].[").append(SEQUENCE_NAME).append("] INCREMENT BY ").append(INCREMENT_BY);
					builder.append(" START WITH ").append(LAST_NUMBER).append(" MAXVALUE ").append(MAX_VALUE).append(" MINVALUE ").append(MIN_VALUE);
				
					if (CYCLE_FLAG.equals("Y")){
						builder.append(" CYCLE ");
					}else{
						builder.append(" NO CYCLE ");
					}
				
					if (CACHE_SIZE == 0){
						builder.append(" NO CACHE ");
					}else{
						builder.append(" CACHE ").append(CACHE_SIZE);
					}
				}else{
					new Exception(msgCode.getCode("C0010"));
				}
				break;
				/*
				switch(DB_VER){
					case "2008": case "2012": case "2014":
						builder.append("CREATE SEQUENCE [").append(SEQUENCE_OWNER).append("].[").append(SEQUENCE_NAME).append("] INCREMENT BY ").append(INCREMENT_BY);
						builder.append(" START WITH ").append(LAST_NUMBER).append(" MAXVALUE ").append(MAX_VALUE).append(" MINVALUE ").append(MIN_VALUE);
					
						if (CYCLE_FLAG.equals("Y")){
							builder.append(" CYCLE ");
						}else{
							builder.append(" NO CYCLE ");
						}
					
						if (CACHE_SIZE == 0){
							builder.append(" NO CACHE ");
						}else{
							builder.append(" CACHE ").append(CACHE_SIZE);
						}				
						break;
					default : 				
						new Exception("Not Supported Version!!!");;
				}
				*/
			case Constant.DB_TYPE.POG : 	
				if (DB_VER.compareTo("9.0") >= 0){
					
					builder.append("CREATE SEQUENCE \"").append(SEQUENCE_OWNER).append("\".\"").append(SEQUENCE_NAME).append("\" INCREMENT BY ").append(INCREMENT_BY);
					builder.append(" START WITH ").append(LAST_NUMBER).append(" MAXVALUE ").append(MAX_VALUE).append(" MINVALUE ").append(MIN_VALUE);
				
					if (CYCLE_FLAG.equals("Y")){
						builder.append(" CYCLE ");
					}else{
						builder.append(" NO CYCLE ");
					}
				
					if (CACHE_SIZE == 0){
						builder.append(" CACHE ").append(1);
					}else{
						builder.append(" CACHE ").append(CACHE_SIZE);
					}				
				}else{
					new Exception(msgCode.getCode("C0010"));;
				}
					
				break;
			case Constant.DB_TYPE.ASE :  
				return null;
			case Constant.DB_TYPE.DB2 :	
				if (DB_VER.compareTo("9.1") >= 0 || DB_VER.compareTo("10.5") <= 0){
					
					builder.append("CREATE SEQUENCE \"").append(SEQUENCE_OWNER).append("\".\"").append(SEQUENCE_NAME).append("\" INCREMENT BY ").append(INCREMENT_BY);
					builder.append(" START WITH ").append(LAST_NUMBER).append(" MAXVALUE ").append(MAX_VALUE).append(" MINVALUE ").append(MIN_VALUE);
				
					if (CYCLE_FLAG.equals("Y")){
						builder.append(" CYCLE ");
					}else{
						builder.append(" NO CYCLE ");
					}
				
					if (CACHE_SIZE == 0){
						builder.append(" NO CACHE ");
					}else{
						builder.append(" CACHE ").append(CACHE_SIZE);
					}				
				}else{
					new Exception(msgCode.getCode("C0010"));;
				}
				
				break;
			default:
				throw new Exception(msgCode.getCode("C0001"));
			}
		}catch(Exception e){
			
			return null;
		}

		
		return builder.toString();
	}
}
