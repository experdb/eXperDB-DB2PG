package com.k4m.experdb.db2pg.convert;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.exception.NotSupportDatabaseTypeException;
import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.vo.DDLStringVO;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.DBUtils;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.db.datastructure.exception.DBTypeNotFoundException;

public abstract class DDLConverter {
	protected ConvertMapper<?> convertMapper;
	protected PriorityBlockingQueue<DDLStringVO> tableQueue= new PriorityBlockingQueue<DDLStringVO>(5, DDLStringVO.getComparator())
			, indexQueue = new PriorityBlockingQueue<DDLStringVO>(5, DDLStringVO.getComparator())
			, constraintsQueue = new PriorityBlockingQueue<DDLStringVO>(5, DDLStringVO.getComparator());
	protected DBConfigInfo dbConfigInfo;
	protected String outputDirectory = ConfigInfo.OUTPUT_DIRECTORY+"ddl/";
	protected List<String> tableNameList = null, excludes = null;
	
	public static DDLConverter getInstance() throws Exception {
		switch(ConfigInfo.SRC_DB_TYPE) {
		case Constant.DB_TYPE.MYSQL :
			return new MySqlDDLConverter();
		default :
			throw new NotSupportDatabaseTypeException(ConfigInfo.SRC_DB_TYPE);
		}
	}
	
	public DDLConverter() throws Exception {
		File dir = new File(outputDirectory);
		if(!dir.exists()){
			LogUtils.info(String.format("%s directory is not existed.", dir.getPath()), DDLConverter.class);
			if(dir.mkdirs()) {
				LogUtils.info(String.format("Success to create %s directory.", dir.getPath()), DDLConverter.class);
			} else {
				LogUtils.error(String.format("Failed to create %s directory.", dir.getPath()), DDLConverter.class);
				System.exit(Constant.ERR_CD.FAILED_CREATE_DIR_ERR);
			}
		}
		this.dbConfigInfo = new DBConfigInfo();
		dbConfigInfo.SERVERIP = ConfigInfo.SRC_HOST;
		dbConfigInfo.PORT = String.valueOf(ConfigInfo.SRC_PORT);
		dbConfigInfo.USERID = ConfigInfo.SRC_USER;
		dbConfigInfo.DB_PW = ConfigInfo.SRC_PASSWORD;
		dbConfigInfo.DBNAME = ConfigInfo.SRC_DATABASE;
		dbConfigInfo.DB_TYPE= ConfigInfo.SRC_DB_TYPE;
		dbConfigInfo.CHARSET = ConfigInfo.SRC_DB_CHARSET;
		dbConfigInfo.SCHEMA_NAME = ConfigInfo.SRC_SCHEMA;
		if (dbConfigInfo.SCHEMA_NAME == null && dbConfigInfo.SCHEMA_NAME.trim().equals("")) dbConfigInfo.SCHEMA_NAME = dbConfigInfo.USERID;
		DBCPPoolManager.setupDriver(dbConfigInfo, Constant.POOLNAME.SOURCE.name(), 1);
		tableNameList = ConfigInfo.SRC_ALLOW_TABLES;
		if(tableNameList == null){
			tableNameList = DBUtils.getTableNames(ConfigInfo.TABLE_ONLY,Constant.POOLNAME.SOURCE.name(), dbConfigInfo);
		}
		if(excludes!= null) {
			for(int eidx=0;eidx < excludes.size(); eidx++) {
				String exclude = excludes.get(eidx);
				for(String tableName : tableNameList) {
					if(exclude.equals(tableName)){
						tableNameList.remove(exclude);
						break;
					}
				}
			}
		}
	}
	
	
	
	public abstract void start() throws DBTypeNotFoundException, IOException ;

}