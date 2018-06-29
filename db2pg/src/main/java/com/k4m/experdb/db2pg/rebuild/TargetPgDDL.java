package com.k4m.experdb.db2pg.rebuild;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.QueryMaker;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;

public class TargetPgDDL {
	private List<String> idxCreateList;
	private List<String> idxDropList;
	private List<String> fkCreateList;
	private List<String> fkDropList;
	private QueryMaker psm;
	
	public TargetPgDDL(){
		psm = new QueryMaker("/tar_mapper.xml");
		idxCreateList = new ArrayList<String>();
		idxDropList = new ArrayList<String>();
		fkCreateList = new ArrayList<String>();
		fkDropList = new ArrayList<String>();
		
		DBConfigInfo tarPgConf = new DBConfigInfo();
		tarPgConf.SERVERIP = ConfigInfo.TAR_HOST;
		tarPgConf.DB_TYPE = Constant.DB_TYPE.POG;
		tarPgConf.PORT = Integer.toString(ConfigInfo.TAR_PORT);
		tarPgConf.DBNAME = ConfigInfo.TAR_DATABASE;
		tarPgConf.SCHEMA_NAME = ConfigInfo.TAR_SCHEMA;
		tarPgConf.USERID = ConfigInfo.TAR_USER;
		tarPgConf.DB_PW = ConfigInfo.TAR_PASSWORD;
		tarPgConf.CHARSET = ConfigInfo.TAR_DB_CHARSET;
		try {
			DBCPPoolManager.setupDriver(tarPgConf, Constant.POOLNAME.TARGET.name(), 1);
			LogUtils.info("[GET_DATABASE_INFORM_START]",TargetPgDDL.class);
			
			Connection connection = DBCPPoolManager.getConnection(Constant.POOLNAME.TARGET.name());
			Statement stat = connection.createStatement();
			ResultSet rs = null;
			try {
				LogUtils.info("[GET_CURRENT_SCHEMA_START]",TargetPgDDL.class);
				rs = stat.executeQuery(psm.getQuery("GET_PG_CURRENT_SCHEMA",Constant.DB_TYPE.POG, Double.parseDouble(tarPgConf.DB_VER)));
				rs.next();
				tarPgConf.SCHEMA_NAME = rs.getString("schema");
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				LogUtils.info("[GET_CURRENT_SCHEMA_END]",TargetPgDDL.class);
			}
			try {
				LogUtils.info("[GET_INDEX_INFORM_START]",TargetPgDDL.class);
				rs = stat.executeQuery(psm.getQuery("GET_PG_IDX_DDL",Constant.DB_TYPE.POG, Double.parseDouble(tarPgConf.DB_VER)));
				while(rs.next()){
					idxCreateList.add(rs.getString("CREATE_DDL_SCRIPT"));
					idxDropList.add(rs.getString("DROP_DDL_SCRIPT"));
				}
				rs.close();
			} catch (Exception e){
				throw(new Exception("[GET_INDEX_INFORM_ERROR]",e));
			} finally {
				LogUtils.info("[GET_INDEX_INFORM_END]",TargetPgDDL.class);
			}
			try {
				LogUtils.info("[GET_FK_INFORM_START]",TargetPgDDL.class);
				rs = stat.executeQuery(psm.getQuery("GET_PG_FK_DDL",Constant.DB_TYPE.POG,Double.parseDouble(tarPgConf.DB_VER)));
				while(rs.next()){
					fkCreateList.add(rs.getString("CREATE_DDL_SCRIPT"));
					fkDropList.add(rs.getString("DROP_DDL_SCRIPT"));
				}
				rs.close();
			} catch (Exception e){
				throw(new Exception("[GET_FK_INFORM_ERROR]",e));
			} finally {
				LogUtils.info("[GET_FK_INFORM_END]",TargetPgDDL.class);
			}
			stat.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			LogUtils.info("[GET_DATABASE_INFORM_END]",TargetPgDDL.class);
		}
	}

	public List<String> getIdxCreateList() {
		return idxCreateList;
	}

	public List<String> getIdxDropList() {
		return idxDropList;
	}

	public List<String> getFkCreateList() {
		return fkCreateList;
	}

	public List<String> getFkDropList() {
		return fkDropList;
	}
	
	
}
