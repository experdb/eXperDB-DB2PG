package com.k4m.experdb.db2pg.convert.make;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.DDLString;
import com.k4m.experdb.db2pg.convert.pattern.SqlPattern;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.table.key.CLUSTER;
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
	
	public List<DDLString> makeCreateTable(Table table) {
		List<DDLString> ddlStringVOs = new LinkedList<DDLString>();
		List<DDLString> tmpStringVOs = new LinkedList<DDLString>();
		StringBuilder ctsb = new StringBuilder();
		StringBuilder tmpsb = new StringBuilder();
		ctsb.append("CREATE TABLE \"");
		ctsb.append(table.getName());
		ctsb.append("\" (");
		boolean isFirst = true;
		for (Column column : table.getColumns()) {
			if (!isFirst) {
				ctsb.append(", ");
			} else {
				isFirst = !isFirst;
			}
			ctsb.append(column.getName());
			ctsb.append(' ');
			if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MYSQL) && SqlPattern.check(column.getType(), SqlPattern.MYSQL.ENUM)) {
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
			} else {
				ctsb.append(column.getType());
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
				ctsb.append(seqName);
				ctsb.append("')");
				tmpsb.append("CREATE SEQUENCE \"");
				tmpsb.append(seqName);
				tmpsb.append('"');
				tmpsb.append(String.format(" INCREMENT %d MINVALUE %d START %d", column.getSeqIncValue(), column.getSeqMinValue(), column.getSeqStart()));
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.SEQUENCE).setPriority(2));
				tmpsb.setLength(0);
			}
			
			if (column.getComment() != null && !column.getComment().equals("")) {
				tmpsb.append("COMMENT ON COLUMN ");
				if(table.getName() != null && !table.getName().equals("")) {
					tmpsb.append('"');
					tmpsb.append(table.getName());
					tmpsb.append("\".");
				}
				tmpsb.append('"');
				tmpsb.append(column.getName());
				tmpsb.append('"');
				tmpsb.append(" IS '");
				tmpsb.append(column.getComment());
				tmpsb.append('\'');
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.COMMENT).setPriority(5));
				tmpsb.setLength(0);
			}
		}//end column
		
		for(Key<?> key : table.getKeys()) {
			switch(key.getType()) {
			case PRIMARY:
				try {
					PrimaryKey pkey = key.unwrap(PrimaryKey.class);
					tmpsb.append("ALTER TABLE \"");
					tmpsb.append(pkey.getTableName());
					tmpsb.append("\" ADD PRIMARY KEY (");
					String columns = pkey.getColumns().toString();
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
					tmpsb.append("ALTER TABLE \"");
					tmpsb.append(fkey.getTableName());
					tmpsb.append("\" ADD CONSTRAINT \"");
					tmpsb.append(fkey.getName());
					tmpsb.append("\" FOREIGN KEY (");
					String columns = fkey.getColumns().toString();
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(") REFERENCES \"");
					tmpsb.append(fkey.getRefTable());
					tmpsb.append("\" (");
					columns = fkey.getRefColumns().toString();
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(")");
					if(fkey.getRefDef() != null) {
						if(fkey.getRefDef().getDelete() != null) {
							tmpsb.append(" ");
							tmpsb.append(fkey.getRefDef().getDelete().getAction());
						}
						if(fkey.getRefDef().getMatch() != null) {
							tmpsb.append(" ");
							tmpsb.append(fkey.getRefDef().getMatch().getType());
						}
						if(fkey.getRefDef().getUpdate() != null) {
							tmpsb.append(" ");
							tmpsb.append(fkey.getRefDef().getUpdate().getAction());
						}
					}
					tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
							.setCommandType(COMMAND_TYPE.FOREIGN_KEY).setPriority(2));
					tmpsb.setLength(0);
				} catch (TableKeyException e) {
					e.printStackTrace();
				}
				break;
			case UNIQUE:
				try {
					UniqueKey ukey = key.unwrap(UniqueKey.class);
					tmpsb.append("CREATE UNIQUE INDEX \"");
					tmpsb.append(ukey.getTableName());
					tmpsb.append("_");
					tmpsb.append(ukey.getName());
					tmpsb.append("\" ON \"");
					tmpsb.append(ukey.getTableName());
					tmpsb.append("\" (");
					String columns = ukey.getColumns().toString();
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
					tmpsb.append(nkey.getTableName());
					tmpsb.append("_");
					tmpsb.append(nkey.getName());
					tmpsb.append("\" ON \"");
					tmpsb.append(nkey.getTableName());
					tmpsb.append("\" (");
					String columns = nkey.getColumns().toString();
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
					CLUSTER cluster = key.unwrap(CLUSTER.class);
					tmpsb.append("CLUSTER  \"");
					tmpsb.append(cluster.getTableName());
					tmpsb.append("\" USING \"");
					tmpsb.append(cluster.getTableName()+"_"+cluster.getIndex_name());
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
			tmpsb.append(table.getName());
			tmpsb.append("\" IS '");
			tmpsb.append(table.getComment());
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
