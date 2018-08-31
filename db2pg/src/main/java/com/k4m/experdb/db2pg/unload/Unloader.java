package com.k4m.experdb.db2pg.unload;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.db.DBUtils;
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;

public class Unloader {
	
//	public int  lobPreFetchSize = -1,rownum = -1, tableParallel = 1, FetchSize = 3000, LoadParallel = 1;
//	String OUTPUT_FILE_NAME = null,configFile = null, outputDirectory = "./",charset = "MS949"
//			,targetSchema = null, where = null;
//	boolean truncate = false, tableOnly = true;
	List<String> tableNameList = null, excludes = null;
	List<SelectQuery> selectQuerys = new ArrayList<SelectQuery>();
//	StringType strType = StringType.original;
	long startTime;
	
	public Unloader () {
	}
	
	
	public void start() {
		try {
			if(!ConfigInfo.SELECT_QUERIES_FILE.equals("")) {
				loadSelectQuery(ConfigInfo.SELECT_QUERIES_FILE);
			}
			
			if(ConfigInfo.SRC_EXCLUDE_TABLES != null)
				excludes = ConfigInfo.SRC_EXCLUDE_TABLES;
			startTime = System.currentTimeMillis();
			
			LogUtils.debug("START UNLOADER !!!",Unloader.class);
			
			
			if (ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME == null && ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.trim().equals("")) ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME = ConfigInfo.SRC_DB_CONFIG.USERID;
			ExecutorService executorService = Executors.newFixedThreadPool(ConfigInfo.SRC_TABLE_SELECT_PARALLEL);
			
			DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG, Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_TABLE_SELECT_PARALLEL);

			List<String> selSqlList = new ArrayList<String>();
			tableNameList = ConfigInfo.SRC_ALLOW_TABLES;
			if(tableNameList == null){
				tableNameList = DBUtils.getTableNames(ConfigInfo.TABLE_ONLY,Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_DB_CONFIG);
			}
			
			if(excludes!= null)
			for(int eidx=0;eidx < excludes.size(); eidx++) {
				String exclude = excludes.get(eidx);
				for(String tableName : tableNameList) {
					if(exclude.equals(tableName)){
						tableNameList.remove(exclude);
						break;
					}
				}
			}
			
			Map<String,Object> params = new HashMap<String,Object>();
			for (String tableName : tableNameList) {
				String schema = "", replaceTableName, where ="";
				if (ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME!=null && !ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.equals("")) {
					schema = ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME+".";
				}
				if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
					replaceTableName = "`"+tableName+"`";
				} else {
					replaceTableName =  "\""+tableName+"\"";
				}
				
				
				if(ConfigInfo.SRC_WHERE!=null && !ConfigInfo.SRC_WHERE.equals("")) {
					where = "WHERE "+ConfigInfo.SRC_WHERE;
				}
				selSqlList.add(String.format("SELECT * FROM %s%s %s", schema,replaceTableName,where));
			}
			params.clear();
			int jobSize = 0;
			if(selSqlList!=null) {
				jobSize += selSqlList.size();
			}
			if(selectQuerys!=null) {
				jobSize += selectQuerys.size();
			}
			List<ExecuteQuery> jobList = new ArrayList<ExecuteQuery>(jobSize);
			
			
			if(selSqlList != null) {
				for(int i=0; i<selSqlList.size(); i++){
	        		ExecuteQuery eq = new ExecuteQuery(Constant.POOLNAME.SOURCE.name(), selSqlList.get(i), tableNameList.get(i), ConfigInfo.SRC_DB_CONFIG);
	        		jobList.add(eq);
	        		executorService.execute(eq);
				}
			}
			if(selectQuerys != null) {
				for(int i=0; i<selectQuerys.size(); i++) {
					ExecuteQuery eq = new ExecuteQuery(Constant.POOLNAME.SOURCE.name(), selectQuerys.get(i).query, selectQuerys.get(i).name, ConfigInfo.SRC_DB_CONFIG);
	        		jobList.add(eq);
	        		executorService.execute(eq);
				}
			}
			

			executorService.shutdown();
			while(!executorService.awaitTermination(500, TimeUnit.MICROSECONDS)){
				continue;
			}
        	long estimatedTime = System.currentTimeMillis() - startTime;
        	
        		   	
//        	for(int i=0;i<jobList.size();i++) {
//        		LogUtils.info("COMPLETE UNLOAD (TABLE_NAME : " + jobList.get(i).getTableName() + ", ROWNUM : " + jobList.get(i).getRowCnt() + ") !!!",Unloader.class);
//        	}
        	try {
	        	File sqlFile = new File(ConfigInfo.OUTPUT_DIRECTORY+"import.sql");
	        	FileOutputStream fos = new FileOutputStream(sqlFile);
	        	fos.write(String.format("SET client_encoding TO '%s';\n\n",ConfigInfo.TAR_DB_CONFIG.CHARSET).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
	        	fos.write("\\set ON_ERROR_STOP OFF\n\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
	        	fos.write("\\set ON_ERROR_ROLLBACK OFF\n\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
	        	fos.write("\\timing\n\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
	        	for(ExecuteQuery executeQuery : jobList){
//	        		fos.write("BEGIN;\n".getBytes(charset));
//	        		fos.write("\\echo [COPY_START] `date +%Y-%m-%d_%k:%M:%S`\n".getBytes(ConfigInfo.TAR_DB_CHARSET));
	        		fos.write(String.format("\\echo TABLE_NAME(ROW_COUNT) >>> %s.%s(%d)\n"
	        				,DevUtils.classifyString(ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME,ConfigInfo.CLASSIFY_STRING)
	        				,DevUtils.classifyString(executeQuery.getTableName()
	        				,ConfigInfo.CLASSIFY_STRING),executeQuery.getRowCnt()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
	        		fos.write(String.format("ALTER TABLE %s DISABLE TRIGGER USER;\n\n",executeQuery.getTableName()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
	        		fos.write(String.format("\\i ./%s",executeQuery.getTableName().replace("$", "-")+".sql\n\n").getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
	        		fos.write(String.format("ALTER TABLE %s ENABLE TRIGGER USER;\n",executeQuery.getTableName()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
//	        		fos.write("\\echo [COPY_END] `date +%Y-%m-%d_%k:%M:%S`\n\n".getBytes(ConfigInfo.TAR_DB_CHARSET));
//	        		fos.write("\\echo  \n".getBytes(ConfigInfo.TAR_DB_CHARSET));
//	        		fos.write("\\echo  \n".getBytes(ConfigInfo.TAR_DB_CHARSET));
//	        		fos.write("COMMIT;\n\\echo \n\n".getBytes(charset));
	        	}
	        	fos.flush();
	        	fos.close();
	        	LogUtils.debug("[MAKE_ALL_IMPORT_SCRIPT_SUCCESS]",Unloader.class);
        	} catch (Exception e) {
        		LogUtils.error("[MAKE_ALL_IMPORT_SCRIPT_FAIL]",Unloader.class,e);
        	}
        	LogUtils.debug("\n",Unloader.class);
        	LogUtils.info("[SUMMARY_INFO]",Unloader.class);
        	
        	StringBuffer sb = new StringBuffer();
        	int failCnt = 0;
    		for(int i=0;i<jobList.size();i++) {
    			sb.setLength(0);
    			sb.append("TABLE_NAME : ");
    			sb.append(jobList.get(i).getTableName());
    			sb.append(", ROWNUM : ");
    			sb.append(String.valueOf(jobList.get(i).getRowCnt()));
    			sb.append(", STATE : ");
    			if(jobList.get(i).isSuccess()){
    				sb.append("SUCCESS");
    			} else {
    				sb.append("FAILURE");
    				failCnt++;
    			}
//    			sb.append('\n');
    			LogUtils.info(sb.toString(),Unloader.class);
    		}
    		LogUtils.info(String.format("[TOTAL_INFO] SUCCESS : %d / FAILURE : %d / TOTAL: %d",jobList.size()-failCnt,failCnt,jobList.size()),Unloader.class);
    		LogUtils.info("[ELAPSED_TIME] " + makeElapsedTimeString(estimatedTime/1000),Unloader.class);
		}catch(Exception e){
			LogUtils.error("EXCEPTION!!!!",Unloader.class,e);
			System.exit(Constant.ERR_CD.UNKNOWN_ERR);
		}
	}
	
	private String makeElapsedTimeString(long elapsedTime) {
		StringBuilder sb = new StringBuilder();
		if(elapsedTime>=60*60) {
			int hour = (int)(elapsedTime/(60*60));
			sb.append(hour);
			sb.append("h ");
			elapsedTime = elapsedTime - hour * 60 * 60;
		} 
		if(elapsedTime>=60) {
			int min = (int)(elapsedTime/60);
			sb.append(min);
			sb.append("m ");
			elapsedTime = elapsedTime - min * 60;
		} 

		sb.append(elapsedTime);
		sb.append("s");
		return sb.toString();
	}
	
	
	private void loadSelectQuery(String queryFilePath) {
		LogUtils.debug("[SELECT_QUERY_LOAD_START]",Unloader.class);
		try {
			File queryFile = new File(queryFilePath);
			if(queryFile.exists()) {
				InputSource is = new InputSource(new FileReader(queryFile));
				
				Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(is);
				XPath xpath = XPathFactory.newInstance().newXPath();
				String expression = "/QUERIES";
		
				NodeList rootNodeList = (NodeList) xpath.compile(expression).evaluate(document, XPathConstants.NODESET);
				NodeList childNodeList = rootNodeList.item(0).getChildNodes();
				String textContent = null, nodeName = null;
				for(int i=0; i<childNodeList.getLength(); i++) {
					Node element = childNodeList.item(i);
					if(element.getNodeType() == Node.ELEMENT_NODE) {
						nodeName = element.getNodeName().toUpperCase();
						if(nodeName.equals("QUERY")){
							String queryName=null, query=null;
							NodeList queryElements = element.getChildNodes();
							for(int queryElemIdx =0;queryElemIdx < queryElements.getLength(); queryElemIdx++) {
								Node queryElement = queryElements.item(queryElemIdx);
								nodeName = queryElement.getNodeName().toUpperCase();
								if(nodeName.equals("NAME")) {
									textContent = queryElement.getTextContent().trim();
									queryName = !textContent.trim().equals("")?textContent:null;
								} else if (nodeName.equals("SELECT")) {
									textContent = queryElement.getTextContent().trim();
									int rmIdx = -1;
									if((rmIdx=textContent.indexOf(";")) != -1) {
										textContent = textContent.substring(0,rmIdx);
									}
									query = !textContent.trim().equals("")?textContent:null;
								}
								if(queryName!=null && query!=null) break;
							}
							if(queryName!=null && query!=null) {
								SelectQuery selectQuery = new SelectQuery(queryName, query);
								selectQuerys.add(selectQuery);
							}
						}
					}
				}
				LogUtils.debug("[SELECT_QUERY_LOAD_SUCCESS]",Unloader.class);
			} else {
				LogUtils.warn("[SELECT_QUERY_FILE_NOT_FOUND]",Unloader.class);
			}
		} catch ( Exception e ) {
			LogUtils.error("[SELECT_QUERY_LOAD_FAIL]",Unloader.class,e);
		} finally {
			LogUtils.debug("[SELECT_QUERY_LOAD_END]",Unloader.class);
		}
	}
	
	
	
	private class SelectQuery {
		String name,query;

		public SelectQuery(String name, String query) {
			super();
			this.name = name;
			this.query = query;
		}
		
	}
	
	
}
