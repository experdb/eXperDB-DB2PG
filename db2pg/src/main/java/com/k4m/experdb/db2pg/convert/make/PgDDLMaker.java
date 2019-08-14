package com.k4m.experdb.db2pg.convert.make;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.DDLString;
import com.k4m.experdb.db2pg.convert.pattern.SqlPattern;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Sequence;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.table.key.Cluster;
import com.k4m.experdb.db2pg.convert.table.key.ForeignKey;
import com.k4m.experdb.db2pg.convert.table.key.Key;
import com.k4m.experdb.db2pg.convert.table.key.NormalKey;
import com.k4m.experdb.db2pg.convert.table.key.PrimaryKey;
import com.k4m.experdb.db2pg.convert.table.key.UniqueKey;
import com.k4m.experdb.db2pg.convert.table.key.exception.TableKeyException;
import com.k4m.experdb.db2pg.convert.type.COMMAND_TYPE;
import com.k4m.experdb.db2pg.convert.type.DDL_TYPE;

public class PgDDLMaker<T> {
	private T t;
	private String dbType;
	private DDL_TYPE ddlType;
	
	public PgDDLMaker(DDL_TYPE ddlType) {
		this.ddlType = ddlType;
		dbType = Constant.DB_TYPE.POG;
	}

	public PgDDLMaker<T> setting(T t) {
		this.t = t;
		return this;
	}

	public List<DDLString> make() {
		switch (ddlType) {
		case CREATE:
			if (t instanceof Table)
				return makeCreateTable((Table) t);
			break;
		case ALTER:
			break;
		case DROP:
			break;
		case RENAME:
			break;
		case TRUNCATE:
			break;
		case UNKNOWN:
			break;
		default:
			break;
		}
		return null;
	}

	public String getDbType() {
		return dbType;
	}

	public DDL_TYPE getDDLType() {
		return ddlType;
	}
	
	// SRC_SRC_CLASSIFY_STRING 옵션에 따라 대문자, 소문자, 원본 지정을 할 수 있게끔 DevUtils.classifyString(String,String) 함수 사용
	public List<DDLString> makeCreateTable(Table table) {
		List<DDLString> ddlStringVOs = new LinkedList<DDLString>();
		List<DDLString> tmpStringVOs = new LinkedList<DDLString>();
		StringBuilder ctsb = new StringBuilder();
		StringBuilder tmpsb = new StringBuilder();
		ctsb.append("CREATE TABLE \"");
		ctsb.append(DevUtils.classifyString(table.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
		ctsb.append("\" (\"");
		boolean isFirst = true;
		for (Column column : table.getColumns()) {
			if (!isFirst) {
				ctsb.append(", \"");
			} else {
				isFirst = !isFirst;
			}
			ctsb.append(DevUtils.classifyString(column.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
			ctsb.append("\" ");
			if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MYS) && SqlPattern.check(column.getType(), SqlPattern.MYS.ENUM)) {
				String typeName = String.format("%s_%s_enum", table.getName(), column.getName());
				tmpsb.append("CREATE TYPE \"");
				tmpsb.append(typeName);
				tmpsb.append("\" AS ");
				tmpsb.append(column.getType());
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.TYPE).setPriority(1));
				tmpsb.setLength(0);
				ctsb.append(typeName);
				table.alertComments().add(MessageFormat.format("/*"
						+ "\n * MySQL {0}.{1} table''s {2} column type is enum."
						+ "\n * But, PostgresQL has needed enum type create."
						+ "\n * So, eXperDB-DB2PG is automatically enum type create."
						+ "\n * TypeName : {1}_{2}\n */", table.getSchemaName(),table.getName(),column.getName()));
			}else if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MSS) && SqlPattern.check(column.getType(), SqlPattern.MYS.ENUM)) {
				String typeName = String.format("%s_%s_enum", table.getName(), column.getName());
				tmpsb.append("CREATE TYPE \"");
				tmpsb.append(typeName);
				tmpsb.append("\" AS ");
				tmpsb.append(column.getType());
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.TYPE).setPriority(1));
				tmpsb.setLength(0);
				ctsb.append(typeName);
				table.alertComments().add(MessageFormat.format("/*"
						+ "\n * MS-SQL {0}.{1} table''s {2} column type is enum."
						+ "\n * But, PostgresQL has needed enum type create."
						+ "\n * So, eXperDB-DB2PG is automatically enum type create."
						+ "\n * TypeName : {1}_{2}\n */", table.getSchemaName(),table.getName(),column.getName()));
			}else {
				ctsb.append(column.getType().toLowerCase());
			}

			if (column.isNotNull()) {
				ctsb.append(" NOT NULL");
			}
			
			if (column.getDefaultValue() != null && !column.getDefaultValue().equals("")) {
				ctsb.append(" DEFAULT ");
				ctsb.append(column.getDefaultValue());
			}
			
			// table_column_seq
			if (column.getSeqStart()>0) {
				String seqName = String.format("%s_%s_seq", table.getName(),column.getName());
				ctsb.append(" DEFAULT NEXTVAL('");
				
				ctsb.append(DevUtils.classifyString(seqName,ConfigInfo.SRC_CLASSIFY_STRING));
				ctsb.append("')");
				tmpsb.append("CREATE SEQUENCE \"");
				tmpsb.append(DevUtils.classifyString(seqName,ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append('"');
				tmpsb.append(String.format(" INCREMENT %d MINVALUE %d START %d", column.getSeqIncValue(), column.getSeqMinValue(), column.getSeqStart()));
				
				tmpsb.append("\nALTER SEQUENCE \"");
				tmpsb.append(DevUtils.classifyString(seqName,ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append('"');
				tmpsb.append(String.format(" RESTART WITH %d", column.getSeqStart()));
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.SEQUENCE).setPriority(2));
				tmpsb.setLength(0);
			}
					
			if (column.getComment() != null && !column.getComment().equals("")) {
				tmpsb.append("COMMENT ON COLUMN ");
				if(table.getName() != null && !table.getName().equals("")) {
					tmpsb.append('"');
					tmpsb.append(DevUtils.classifyString(table.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\".");
				}
				tmpsb.append('"');
				tmpsb.append(DevUtils.classifyString(column.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append('"');
				tmpsb.append(" IS '");
				tmpsb.append(column.getComment());
				tmpsb.append('\'');
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.COMMENT).setPriority(5));
				tmpsb.setLength(0);
			}
		}//end column
		
		//ORA -> Sequence
		if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.ORA) && table.getSequence() !=null){
			for(Sequence sequence : table.getSequence()) {
				tmpsb.append("CREATE SEQUENCE \"");
				tmpsb.append(DevUtils.classifyString(sequence.getSeqName(),ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append('"');
				tmpsb.append(String.format(" INCREMENT %d MINVALUE %d START %d", sequence.getSeqIncValue(), sequence.getSeqMinValue(), sequence.getSeqStart()));
				
				tmpsb.append("\nALTER SEQUENCE \"");
				tmpsb.append(DevUtils.classifyString(sequence.getSeqName(),ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append('"');
				tmpsb.append(String.format(" RESTART WITH %d", sequence.getSeqStart()));
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.SEQUENCE).setPriority(2));
				tmpsb.setLength(0);
			}	
		}
		
		for(Key<?> key : table.getKeys()) {
			switch(key.getType()) {
			case PRIMARY:
				try {
					PrimaryKey pkey = key.unwrap(PrimaryKey.class);
					tmpsb.append("ALTER TABLE \"");
					tmpsb.append(DevUtils.classifyString(pkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" ADD PRIMARY KEY (");
					String columns = DevUtils.classifyString(pkey.getColumns().toString(),ConfigInfo.SRC_CLASSIFY_STRING);
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(")");
					tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
							.setCommandType(COMMAND_TYPE.PRIMARY_KEY).setPriority(1));
					tmpsb.setLength(0);
				} catch (TableKeyException e) {
					e.printStackTrace();
				}
				break;
			case FOREIGN:
				try {
					ForeignKey fkey = key.unwrap(ForeignKey.class);
					if(fkey.getRefTable() != null){
					tmpsb.append("ALTER TABLE \"");
					tmpsb.append(DevUtils.classifyString(fkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" ADD CONSTRAINT \"");
					tmpsb.append(fkey.getName().toLowerCase());
					tmpsb.append("\" FOREIGN KEY (");
					String columns = DevUtils.classifyString(fkey.getColumns().toString(),ConfigInfo.SRC_CLASSIFY_STRING);
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(") REFERENCES \"");
					if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MSS)){
						String[] values = fkey.getRefTable().split("_");
						tmpsb.append(values[1]);
						tmpsb.append("\" (");
						columns = fkey.getRefColumns().toString();
					}else{
						tmpsb.append(DevUtils.classifyString(fkey.getRefTable(),ConfigInfo.SRC_CLASSIFY_STRING));
						tmpsb.append("\" (");
						columns = DevUtils.classifyString(fkey.getRefColumns().toString(),ConfigInfo.SRC_CLASSIFY_STRING);
					}
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(")");
					if(fkey.getRefDef() != null) {
						if(fkey.getRefDef().getMatch() != null) {
							tmpsb.append(" ");
							tmpsb.append(fkey.getRefDef().getMatch().getType());
						}
						if(fkey.getRefDef().getDelete() != null) {
							tmpsb.append(" ");
							tmpsb.append(fkey.getRefDef().getDelete().getAction());
						}						
						if(fkey.getRefDef().getUpdate() != null) {
							tmpsb.append(" ");
							tmpsb.append(fkey.getRefDef().getUpdate().getAction());
						}
					}
					
					if(fkey.getDeferrable() !=null && fkey.getDeferred() !=null){
						tmpsb.append(" "+fkey.getDeferrable());
						tmpsb.append(" INITIALLY ");
						tmpsb.append(fkey.getDeferred());
					}
					tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
							.setCommandType(COMMAND_TYPE.FOREIGN_KEY).setPriority(2));
					tmpsb.setLength(0);
					}
				} catch (TableKeyException e) {
					e.printStackTrace();
				}
				break;
			case UNIQUE:
				try {
					UniqueKey ukey = key.unwrap(UniqueKey.class);
					tmpsb.append("CREATE UNIQUE INDEX \"");
					tmpsb.append(DevUtils.classifyString(ukey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("_");
					tmpsb.append(DevUtils.classifyString(ukey.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" ON \"");
					tmpsb.append(DevUtils.classifyString(ukey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" (");
					String columns = DevUtils.classifyString(ukey.getColumns().toString(),ConfigInfo.SRC_CLASSIFY_STRING);
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(")");
					tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
							.setCommandType(COMMAND_TYPE.INDEX).setPriority(1));
					tmpsb.setLength(0);
				} catch (TableKeyException e) {
					e.printStackTrace();
				}
				break;
			case NORMAL:
				try {
					NormalKey nkey = key.unwrap(NormalKey.class);
					tmpsb.append("CREATE INDEX \"");
					tmpsb.append(DevUtils.classifyString(nkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("_");
					tmpsb.append(DevUtils.classifyString(nkey.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" ON \"");
					tmpsb.append(DevUtils.classifyString(nkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" (");
					String columns = DevUtils.classifyString(nkey.getColumns().toString(),ConfigInfo.SRC_CLASSIFY_STRING);
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(")");
					tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
							.setCommandType(COMMAND_TYPE.INDEX).setPriority(2));
					tmpsb.setLength(0);
				} catch (TableKeyException e) {
					e.printStackTrace();
				}
				break;
			case CLUSTER:
				try {
					Cluster cluster = key.unwrap(Cluster.class);
					tmpsb.append("CLUSTER  \"");
					tmpsb.append(DevUtils.classifyString(cluster.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" USING \"");
					tmpsb.append(DevUtils.classifyString(cluster.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING)+"_"+DevUtils.classifyString(cluster.getIndexName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("\" ");
					tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
							.setCommandType(COMMAND_TYPE.INDEX).setPriority(3));
					tmpsb.setLength(0);
				} catch (TableKeyException e) {
					e.printStackTrace();
				}
			default:
				break;
			}
		}
		

		//comment
		if(table.getComment() != null && !table.getComment().equals(""))  {
			tmpsb.append("COMMENT ON TABLE \"");
			tmpsb.append(DevUtils.classifyString(table.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
			tmpsb.append("\" IS '");
			tmpsb.append(DevUtils.classifyString(table.getComment(),ConfigInfo.SRC_CLASSIFY_STRING));
			tmpsb.append('\'');
			tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
					.setCommandType(COMMAND_TYPE.COMMENT).setPriority(4));
			tmpsb.setLength(0);
		}
		
		
		ctsb.append(")");
		ddlStringVOs.add(new DDLString().setString(ctsb.toString()).setDDLType(ddlType)
				.setCommandType(COMMAND_TYPE.TABLE).setPriority(3).setAlertComments(table.alertComments()));
		for (DDLString ddlStringVO : tmpStringVOs) {
			ddlStringVOs.add(ddlStringVO);
		}
		ctsb.setLength(0);
		return ddlStringVOs;
	}

}
