package com.k4m.experdb.db2pg.convert.make;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.k4m.experdb.db2pg.db.DBUtils;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.db.impl.MetaExtractWorker;
import com.k4m.experdb.db2pg.work.db.impl.WORK_TYPE;

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
		ctsb.append("CREATE TABLE ");
		ctsb.append(DevUtils.classifyString(table.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
		ctsb.append(" (");
		boolean isFirst = true;
		for (Column column : table.getColumns()) {
			
			//System.out.println(column.getName()+":"+column.toString());
			
			if (!isFirst) {
				ctsb.append(", ");
			} else {
				isFirst = !isFirst;
			}
			ctsb.append(DevUtils.classifyString(column.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
			ctsb.append(" ");
			if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MYS) && SqlPattern.check(column.getType(), SqlPattern.MYS.ENUM)) {
				String typeName = String.format("%s_%s_enum", table.getName(), column.getName());
				tmpsb.append("CREATE TYPE ");
				tmpsb.append(typeName);
				tmpsb.append(" AS ");
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
				tmpsb.append("CREATE TYPE ");
				tmpsb.append(typeName);
				tmpsb.append(" AS ");
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
				tmpsb.append("CREATE SEQUENCE ");
				tmpsb.append(DevUtils.classifyString(seqName,ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append(String.format(" INCREMENT %d MINVALUE %d START %d;", column.getSeqIncValue(), column.getSeqMinValue(), column.getSeqStart()));
				
				tmpsb.append("\nALTER SEQUENCE ");
				tmpsb.append(DevUtils.classifyString(seqName,ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append(String.format(" RESTART WITH %d", column.getSeqStart()));
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.SEQUENCE).setPriority(2));
				tmpsb.setLength(0);
			}
					
			if (column.getComment() != null && !column.getComment().equals("")) {
				tmpsb.append("COMMENT ON COLUMN ");
				if(table.getName() != null && !table.getName().equals("")) {
					tmpsb.append(DevUtils.classifyString(table.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(".");
				}
				tmpsb.append(DevUtils.classifyString(column.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append(" IS '");
				tmpsb.append(column.getComment());
				tmpsb.append("'");
				tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
						.setCommandType(COMMAND_TYPE.COMMENT).setPriority(5));
				tmpsb.setLength(0);
			}
		}//end column
		
		//ORA -> Sequence
		if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.ORA) || ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.TBR) && table.getSequence() !=null){
			for(Sequence sequence : table.getSequence()) {
				tmpsb.append("CREATE SEQUENCE ");
				tmpsb.append(DevUtils.classifyString(sequence.getSeqName(),ConfigInfo.SRC_CLASSIFY_STRING));
				tmpsb.append(String.format(" INCREMENT %d MINVALUE %d START %d MAXVALUE %s;", sequence.getSeqIncValue(), sequence.getSeqMinValue(), sequence.getSeqStart(), sequence.getSeqMaxalue()));
				tmpsb.append("\nALTER SEQUENCE ");
				tmpsb.append(DevUtils.classifyString(sequence.getSeqName(),ConfigInfo.SRC_CLASSIFY_STRING));
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
					tmpsb.append("ALTER TABLE ");
					tmpsb.append(DevUtils.classifyString(pkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					
					/*tmpsb.append(" ADD PRIMARY KEY (");
					String columns = DevUtils.classifyString(pkey.getColumns().toString(),ConfigInfo.SRC_CLASSIFY_STRING);
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(")");*/
					
					tmpsb.append(" ADD PRIMARY KEY USING INDEX ");
					tmpsb.append(DevUtils.classifyString(pkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("_");
					tmpsb.append(DevUtils.classifyString(pkey.getIndexName(),ConfigInfo.SRC_CLASSIFY_STRING));
								
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
					tmpsb.append("ALTER TABLE ");
					tmpsb.append(DevUtils.classifyString(fkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(" ADD CONSTRAINT ");
					tmpsb.append(fkey.getName().toLowerCase());
					tmpsb.append(" FOREIGN KEY (");
					String columns = DevUtils.classifyString(fkey.getColumns().toString(),ConfigInfo.SRC_CLASSIFY_STRING);
					tmpsb.append(columns.substring(columns.indexOf("[")+1,columns.indexOf("]")));
					tmpsb.append(") REFERENCES ");
					if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MSS)){

						//System.out.println(fkey.getRefTable());

						/*String[] values = fkey.getRefTable().split("_");
						tmpsb.append(values[1])*/;
						
						tmpsb.append(fkey.getRefTable());
						tmpsb.append(" (");
						columns = fkey.getRefColumns().toString();
					}else{
						tmpsb.append(DevUtils.classifyString(fkey.getRefTable(),ConfigInfo.SRC_CLASSIFY_STRING));
						tmpsb.append(" (");
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
					tmpsb.append("CREATE UNIQUE INDEX ");
					tmpsb.append(DevUtils.classifyString(ukey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("_");
					tmpsb.append(DevUtils.classifyString(ukey.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(" ON ");
					tmpsb.append(DevUtils.classifyString(ukey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(" (");
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
					tmpsb.append("CREATE INDEX ");
					tmpsb.append(DevUtils.classifyString(nkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append("_");
					tmpsb.append(DevUtils.classifyString(nkey.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(" ON ");
					tmpsb.append(DevUtils.classifyString(nkey.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(" (");
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
					tmpsb.append("CLUSTER  ");
					tmpsb.append(DevUtils.classifyString(cluster.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(" USING ");
					tmpsb.append(DevUtils.classifyString(cluster.getTableName(),ConfigInfo.SRC_CLASSIFY_STRING)+"_"+DevUtils.classifyString(cluster.getIndexName(),ConfigInfo.SRC_CLASSIFY_STRING));
					tmpsb.append(" ");
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
			tmpsb.append("COMMENT ON TABLE ");
			tmpsb.append(DevUtils.classifyString(table.getName(),ConfigInfo.SRC_CLASSIFY_STRING));
			tmpsb.append(" IS '");
			tmpsb.append(table.getComment());
			tmpsb.append("'");
			tmpStringVOs.add(new DDLString().setString(tmpsb.toString()).setDDLType(DDL_TYPE.CREATE)
					.setCommandType(COMMAND_TYPE.COMMENT).setPriority(4));
			tmpsb.setLength(0);
		}
		
		ctsb.append(")");

		// Oracle partition DDL 
		if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.ORA) && table.getPtCnt() > 0) {
			// Partition Function Create
			//DBUtils.getCreateFnLong();
			
			ctsb.append(" PARTITION BY "+table.getPtType() + " (" + table.getPartKeyColumn() + ");\n");
			
			int k=0;
			String rangeStart = "MINVALUE";
			for(Column column : table.getPartColumns()) {
				ctsb.append("CREATE TABLE ");
				ctsb.append(DevUtils.classifyString(column.getPartitionName(),ConfigInfo.SRC_CLASSIFY_STRING));
				ctsb.append(" PARTITION OF " + DevUtils.classifyString(column.getPartitionTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
				if(column.getHighValue() != null && !column.getHighValue().toLowerCase().contains("default")) {
					ctsb.append(" FOR VALUES ");	
				}
				if(column.getPartitioningType().toUpperCase().equals("LIST")) {
					if(column.getHighValue() != null && column.getHighValue().toLowerCase().contains("default")) {
						ctsb.append(" DEFAULT");
					}else {
						ctsb.append("IN  (" + column.getHighValue() + ")");
					}
				}else if(column.getPartitioningType().toUpperCase().equals("RANGE")){
					if(column.getHighValue() != null && column.getHighValue().toLowerCase().contains("default")) {
						ctsb.append(" DEFAULT");
					}else {
						/*Pattern p = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})");
						Matcher m = p.matcher(column.getHighValue());
						String d = "";
						while (m.find()) {
							d = "'"+m.group()+"'";
						}
						if(d.equals("")) d = column.getHighValue();*/
						String d = column.getHighValue();
						if(column.getType().toUpperCase().equals("DATE") && !d.toUpperCase().equals("MAXVALUE") && !d.toUpperCase().equals("MINVALUE")) d = "'"+d+"'";
						ctsb.append("FROM("+rangeStart+") TO (" + d +")");
						rangeStart = d;
					}
				}else if(column.getPartitioningType().toUpperCase().equals("HASH")) {
					if(column.getHighValue() != null && column.getHighValue().toLowerCase().contains("default")) {
						ctsb.append(" DEFAULT");
					}else {
						int remainder = column.getPartitionPosition() - 1;
						ctsb.append("WITH (modulus "+table.getPtCnt()+", remainder "+ remainder +")");
					}
				}
				
				// Sub Partition Exist
				if(table.getPtSubCnt() > 0) {
					ctsb.append(" PARTITION BY "+table.getPtSubType() + " (" + table.getPartSubKeyColumn() + ")");
				}
				k++;
				if(table.getPartColumns().size() != k) {
					ctsb.append(";\n");
				}
			}
			
			// Sub Partition
			if(table.getPtSubCnt() > 0) {
   			
				k=0;
				rangeStart = "MINVALUE";
				for(Column column : table.getSubPartColumns()) {
					if(k == 0) ctsb.append(";\n");
					ctsb.append("CREATE TABLE ");
					ctsb.append(DevUtils.classifyString(column.getSubPartitionName(),ConfigInfo.SRC_CLASSIFY_STRING));
					ctsb.append(" PARTITION OF " + DevUtils.classifyString(column.getPartitionTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
					if(column.getHighValue() != null && !column.getHighValue().toLowerCase().contains("default")) {
						ctsb.append(" FOR VALUES ");	
					}
					if(column.getSubPartitioningType().toUpperCase().equals("LIST")) {
						if(column.getHighValue() != null && column.getHighValue().toLowerCase().contains("default")) {
							ctsb.append(" DEFAULT");
						}else {
							ctsb.append("IN  (" + column.getHighValue() + ")");
						}
					}else if(column.getSubPartitioningType().toUpperCase().equals("RANGE")){
						if(column.getHighValue() != null && column.getHighValue().toLowerCase().contains("default")) {
							ctsb.append(" DEFAULT");
						}else {
							String d = column.getHighValue();
							if(column.getType().toUpperCase().equals("DATE") && !d.toUpperCase().equals("MAXVALUE") && !d.toUpperCase().equals("MINVALUE")) d = "'"+d+"'";
							ctsb.append("FROM("+rangeStart+") TO (" + d +")");
							rangeStart = d;
						}
					}else if(column.getSubPartitioningType().toUpperCase().equals("HASH")) {
						if(column.getHighValue() != null && column.getHighValue().toLowerCase().contains("default")) {
							ctsb.append(" DEFAULT");
						}else {
							int remainder = column.getPartitionPosition() - 1;
							ctsb.append("WITH (modulus "+table.getPtCnt()+", remainder "+ remainder +")");
						}
					}
					k++;
					if(table.getSubPartColumns().size() != k) {
						ctsb.append(";\n");
					}
				}
			}
			
			// Partition Function Drop
			//DBUtils.getDropFnLong();
		}
		
		ddlStringVOs.add(new DDLString().setString(ctsb.toString()).setDDLType(ddlType)
				.setCommandType(COMMAND_TYPE.TABLE).setPriority(3).setAlertComments(table.alertComments()));
		for (DDLString ddlStringVO : tmpStringVOs) {
			ddlStringVOs.add(ddlStringVO);
		}
		ctsb.setLength(0);
		return ddlStringVOs;
	}

}
