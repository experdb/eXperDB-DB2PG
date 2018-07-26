package com.k4m.experdb.db2pg.convert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.db.ConvertDBUtils;
import com.k4m.experdb.db2pg.convert.make.PgDDLMaker;
import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.map.MySqlConvertMapper;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.type.DDL_TYPE;
import com.k4m.experdb.db2pg.convert.vo.ConvertVO;
import com.k4m.experdb.db2pg.convert.vo.DDLStringVO;
import com.k4m.experdb.db2pg.db.datastructure.exception.DBTypeNotFoundException;

public class MySqlDDLConverter extends DDLConverter{

	public MySqlDDLConverter() throws Exception {
		super();
		convertMapper = ConvertMapper.makeConvertMapper(MySqlConvertMapper.class);
	}

	@Override
	public void start() throws DBTypeNotFoundException, IOException {
		DDLStringVO ddlStrVO = null;
		List<Table> tables = ConvertDBUtils.getTableInform(tableNameList,true,Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
		List<String> alertComments = new LinkedList<String>();
		PgDDLMaker<Table> maker = new PgDDLMaker<Table>(DDL_TYPE.CREATE);
		Queue<DDLStringVO> ddlQueue = new LinkedBlockingQueue<DDLStringVO>();
		for(Table table : tables) {
			ConvertDBUtils.setColumnInform(table, Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
			ConvertDBUtils.setConstraintInform(table,Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
			ConvertDBUtils.setKeyInform(table,Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
			for(Column column : table.getColumns()) {
				for(ConvertVO convertVO:convertMapper.getPatternList()) {
					if(convertVO.getPattern().matcher(column.getType()).find()) {
						if(convertVO.getToValue().equals("DECIMAL")) {
							column.setType(String.format("%s(%d,%d)", convertVO.getToValue(),column.getNumericPrecision(),column.getNumericScale()));
						} else if(convertVO.getToValue().equals("BOOLEAN")) {
							if(column.getDefaultValue()!= null)
							if(column.getDefaultValue().equals("1")) {
								column.setDefaultValue("TRUE");
							} else if(column.getDefaultValue().equals("0")) {
								column.setDefaultValue("FALSE");
							}
							column.setType(convertVO.getToValue());
						} else if(convertVO.getToValue().equals("DATE")) {
							String yearRedix = "^(?i)YEAR\\s*\\(?[0-9]*\\)?$";
							Pattern yearPattern = Pattern.compile(yearRedix);
							if(yearPattern.matcher(column.getType()).matches()) {
								table.alertComments().add(MessageFormat.format("/*"
										+ "\n * MySQL Year Type has been changed to PostgresQL Date Type."
										+ "\n * Column : {0}.{1}.{2}"
										+ "\n */", table.getSchemaName(), table.getName(), column.getName()));
							}
							column.setType(convertVO.getToValue());
						} else {
							column.setType(convertVO.getToValue());
						}
						break;
					}
				}
				String onUptRedix = "^(?i)on update \\w*$";
				Pattern onUptPattern = Pattern.compile(onUptRedix);
				if(onUptPattern.matcher(column.getExtra()).matches()) {
					table.alertComments().add(MessageFormat.format("/*"
							+ "\n * MySQL {0}.{1} table''s {2} column has been included on update constraint on table."
							+ "\n * But, PostgresQL''s column isn''t supported on update constraint."
							+ "\n * So, need you will make trigger action on this table."
							+ "\n * Column : {0}.{1}.{2}"
							+ "\n */", table.getSchemaName(), table.getName(), column.getName()));
				}
			}
			maker.setting(table);
			ddlQueue.clear();
			ddlQueue.addAll(maker.make());
			
			while((ddlStrVO = ddlQueue.poll())!= null) {
				if(ddlStrVO.getDDLType() == DDL_TYPE.CREATE) {
					switch(ddlStrVO.getCommandType()) {
					case TYPE: case TABLE: case COMMENT: case SEQUENCE:
						tableQueue.add(ddlStrVO);
						break;
					case FOREIGN_KEY: case PRIMARY_KEY:
						constraintsQueue.add(ddlStrVO);
						break;
					case INDEX:
						indexQueue.add(ddlStrVO);
						break;
					default:
						break;
					}
				}
			}
		}
		
		ByteBuffer fileBuffer = ByteBuffer.allocateDirect(ConfigInfo.BUFFER_SIZE);
		FileChannel fch = null;
		File tableSqlFile = new File(outputDirectory+"/"+dbConfigInfo.DBNAME+"_table.sql");
		FileOutputStream fos = new FileOutputStream(tableSqlFile);
		fch = fos.getChannel();
		
		while((ddlStrVO = tableQueue.poll())!= null) {
			if ( ddlStrVO.getAlertComments() != null ) {
				for(String alertComment : ddlStrVO.getAlertComments()) {
					fileBuffer.put(alertComment.getBytes(ConfigInfo.TAR_DB_CHARSET));
					alertComments.add(alertComment);
					fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CHARSET));
				}
			}
			fileBuffer.put(ddlStrVO.toString().getBytes(ConfigInfo.TAR_DB_CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CHARSET));
			fileBuffer.flip();
			fch.write(fileBuffer);
			fileBuffer.clear();
		}
		fch.close();
		fos.close();
		
		File constraintsSqlFile = new File(outputDirectory+"/"+dbConfigInfo.DBNAME+"_constraints.sql");
		fos = new FileOutputStream(constraintsSqlFile);
		fch = fos.getChannel();
		while((ddlStrVO = constraintsQueue.poll())!= null) {
			fileBuffer.put(ddlStrVO.toString().getBytes(ConfigInfo.TAR_DB_CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CHARSET));
			fileBuffer.flip();
			fch.write(fileBuffer);
			fileBuffer.clear();
		}
		fch.close();
		fos.close();
		
		File indexSqlFile = new File(outputDirectory+"/"+dbConfigInfo.DBNAME+"_index.sql");
		fos = new FileOutputStream(indexSqlFile);
		fch = fos.getChannel();
		while((ddlStrVO = indexQueue.poll())!= null) {
			fileBuffer.put(ddlStrVO.toString().getBytes(ConfigInfo.TAR_DB_CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CHARSET));
			fileBuffer.flip();
			fch.write(fileBuffer);
			fileBuffer.clear();
		}
		fch.close();
		fos.close();
		if(!alertComments.isEmpty()) {
			File alertFile = new File(outputDirectory+"/"+dbConfigInfo.DBNAME+"_alert.log");
			fos = new FileOutputStream(alertFile);
			fch = fos.getChannel();
			for(String alertComment : alertComments) {
				System.out.println(alertComment);
				fileBuffer.put(alertComment.getBytes(ConfigInfo.TAR_DB_CHARSET));
				fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CHARSET));
				fileBuffer.flip();
				fch.write(fileBuffer);
				fileBuffer.clear();
			}
			fch.close();
			fos.close();
		}
	}
	
	
}
