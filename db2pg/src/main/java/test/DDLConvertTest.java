package test;


import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.convert.ConvertObject;
import com.k4m.experdb.db2pg.convert.DDLString;
import com.k4m.experdb.db2pg.convert.db.ConvertDBUtils;
import com.k4m.experdb.db2pg.convert.make.PgDDLMaker;
import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.map.MySqlConvertMapper;
import com.k4m.experdb.db2pg.convert.map.exception.MapperNotFoundException;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.convert.type.DDL_TYPE;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.db.datastructure.exception.DBTypeNotFoundException;

public class DDLConvertTest {
	public static void main(String[] args) {
		new DDLConvertTest();
	}
	
	public DDLConvertTest() {
		try {
			
			ConvertMapper<?> convertMapper = ConvertMapper.makeConvertMapper(MySqlConvertMapper.class);
			DBConfigInfo dbConfigInfo = new DBConfigInfo();
			dbConfigInfo.SERVERIP = "PIDSVR";
			dbConfigInfo.PORT = String.valueOf(3306);
			dbConfigInfo.USERID = "test";
			dbConfigInfo.DB_PW = "1q2w#E$R";
			dbConfigInfo.DBNAME = "employees";
			dbConfigInfo.SCHEMA_NAME="employees";
			dbConfigInfo.DB_TYPE= Constant.DB_TYPE.MYSQL;
			dbConfigInfo.CHARSET = "UTF-8";
			LogUtils.setVerbose(false);
			DBCPPoolManager.setupDriver(dbConfigInfo, Constant.POOLNAME.SOURCE.name(), 1);
			PriorityBlockingQueue<DDLString> tableQueue = new PriorityBlockingQueue<>(20, DDLString.getComparator())
					, tableIndexQueue = new PriorityBlockingQueue<>(5, DDLString.getComparator())
					, tableConstraintsQueue = new PriorityBlockingQueue<>(10, DDLString.getComparator());
			Queue<DDLString> ddlQueue = new LinkedBlockingQueue<DDLString>();
			DDLString ddlStrVO = null;
			
			List<String> tableNames = new ArrayList<>();
//			tableNames.add("titles");
			List<Table> tables = ConvertDBUtils.getTableInform(tableNames,true,Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
			PgDDLMaker<Table> ddlMaker = new PgDDLMaker<Table>(DDL_TYPE.CREATE);
			for(Table table : tables) {
				ConvertDBUtils.setColumnInform(table, Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
				ConvertDBUtils.setConstraintInform(table,Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
				ConvertDBUtils.setKeyInform(table,Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
				for(Column column : table.getColumns()) {
					for(ConvertObject convertVO:convertMapper.getPatternList()) {
						if(convertVO.getPattern().matcher(column.getType()).find()) {
							column.setType(convertVO.getToValue());
							break;
						}
					}
				}
				ddlMaker.setting(table);
				ddlQueue.clear();
				ddlQueue.addAll(ddlMaker.make());
				while((ddlStrVO = ddlQueue.poll())!= null) {
					if(ddlStrVO.getDDLType() == DDL_TYPE.CREATE) {
						switch(ddlStrVO.getCommandType()) {
						case TYPE: case TABLE: case COMMENT: case SEQUENCE:
							tableQueue.add(ddlStrVO);
							break;
						case FOREIGN_KEY: case PRIMARY_KEY:
							tableConstraintsQueue.add(ddlStrVO);
							break;
						case INDEX:
							tableIndexQueue.add(ddlStrVO);
							break;
						default:
							break;
						}
					}
				}
			}
			
			ByteBuffer fileBuffer = ByteBuffer.allocateDirect(1024*1024*1);
			FileChannel fch = null;
			File tableSqlFile = new File(dbConfigInfo.DBNAME+"_table.sql");
			FileOutputStream fos = new FileOutputStream(tableSqlFile);
			fch = fos.getChannel();
			
			while((ddlStrVO = tableQueue.poll())!= null) {
				fileBuffer.put(ddlStrVO.toString().getBytes());
				fileBuffer.put("\n".getBytes());
				fileBuffer.flip();
				fch.write(fileBuffer);
				fileBuffer.clear();
			}
			fch.close();
			fos.close();
			
			File constraintsSqlFile = new File(dbConfigInfo.DBNAME+"_constraints.sql");
			fos = new FileOutputStream(constraintsSqlFile);
			fch = fos.getChannel();
			while((ddlStrVO = tableConstraintsQueue.poll())!= null) {
				fileBuffer.put(ddlStrVO.toString().getBytes());
				fileBuffer.put("\n".getBytes());
				fileBuffer.flip();
				fch.write(fileBuffer);
				fileBuffer.clear();
			}
			fch.close();
			fos.close();
			
			File indexSqlFile = new File(dbConfigInfo.DBNAME+"_index.sql");
			fos = new FileOutputStream(indexSqlFile);
			fch = fos.getChannel();
			while((ddlStrVO = tableIndexQueue.poll())!= null) {
				fileBuffer.put(ddlStrVO.toString().getBytes());
				fileBuffer.put("\n".getBytes());
				fileBuffer.flip();
				fch.write(fileBuffer);
				fileBuffer.clear();
			}
			fch.close();
			fos.close();
		} catch (MapperNotFoundException e) {
			LogUtils.error(e.getMessage(), DDLConvertTest.class,e);
		} catch (DBTypeNotFoundException e) {
			LogUtils.error(e.getMessage(), DDLConvertTest.class,e);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
}
