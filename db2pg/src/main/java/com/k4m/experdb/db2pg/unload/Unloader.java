package com.k4m.experdb.db2pg.unload;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
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
import com.k4m.experdb.db2pg.common.DevUtils;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.db.DBUtils;

public class Unloader {
	private File impSql = null;
	List<SelectQuery> selectQuerys = new ArrayList<SelectQuery>();

	long startTime;
	
	public Unloader () {
	}
	
	/**
	 * 태이블 조회
	 * @throws Exception
	 */
	private List<String> makeTableList() throws Exception {
		List<String> excludes = ConfigInfo.SRC_EXCLUDE_TABLES;
		
		List<String>  tableNameList = ConfigInfo.SRC_ALLOW_TABLES;
		
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
		
		return tableNameList;
	}
	
	private String getConvertObjectName(String objName) throws Exception {
		String strReplaceTableName = "";
		
		if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MYSQL)) {
			strReplaceTableName = "`" + objName + "`";
		} else {
			strReplaceTableName =  "\"" + objName + "\"";
		}
		
		return strReplaceTableName;
	}
	
	private String getWhere() throws Exception {
		String strWhere ="";
		
		if(ConfigInfo.SRC_WHERE!=null && !ConfigInfo.SRC_WHERE.equals("")) {
			strWhere = "WHERE "+ConfigInfo.SRC_WHERE;
		}
		return strWhere;
	}
	
	private void setSchemaNameCheck() throws Exception {
		if (ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME == null && ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.trim().equals("")) {
			ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME = ConfigInfo.SRC_DB_CONFIG.USERID;
		}
	}
	
	public void start() {
		
		ExecutorService executorService = Executors.newFixedThreadPool(ConfigInfo.SRC_TABLE_SELECT_PARALLEL);
		
		try {

			
			if (ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME==null && ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.equals("")) {
				LogUtils.error("SCHEMA_NAME NOT FOUND", Unloader.class);
				System.exit(0);
			}
			startTime = System.currentTimeMillis();
			
			LogUtils.debug("START UNLOADER !!!", Unloader.class);

			setSchemaNameCheck();
			
			
			
			//DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG, Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_TABLE_SELECT_PARALLEL);

			List<String> selSqlList = new ArrayList<String>();

			List<String> tableNameList =  null;
			
			if(!ConfigInfo.SELECT_QUERIES_FILE.equals("")) {
				loadSelectQuery(ConfigInfo.SELECT_QUERIES_FILE);
			} 

				
			//Table Select
			tableNameList = makeTableList();
			for (String tableName : tableNameList) {
				String schema = ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME!=null && !ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.equals("") 
									? getConvertObjectName(ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME)+"." 
									: "" ;
				String replaceTableName = getConvertObjectName(tableName);
				String where = getWhere();

				selSqlList.add(String.format("SELECT * FROM %s%s %s", schema, replaceTableName, where));
			}


			int jobSize = 0;
			if(selSqlList != null) {
				jobSize += selSqlList.size();
			}
			
			if(selectQuerys != null) {
				jobSize += selectQuerys.size();
			}
			List<ExecuteDataTransfer> jobList = new ArrayList<ExecuteDataTransfer>(jobSize);
			
			
			if(selSqlList != null) {
				for(int i=0; i<selSqlList.size(); i++){
	        		ExecuteDataTransfer eq = new ExecuteDataTransfer(Constant.POOLNAME.SOURCE.name(), selSqlList.get(i), tableNameList.get(i), ConfigInfo.SRC_DB_CONFIG);
	        		jobList.add(eq);
	        		executorService.execute(eq);
				}
			}
			
			if(selectQuerys != null) {
				for(int i=0; i<selectQuerys.size(); i++) {
					ExecuteDataTransfer eq = new ExecuteDataTransfer(Constant.POOLNAME.SOURCE.name(), selectQuerys.get(i).query, selectQuerys.get(i).name, ConfigInfo.SRC_DB_CONFIG);
	        		jobList.add(eq);
	        		executorService.execute(eq);
				}
			}
			

			executorService.shutdown();
			while(!executorService.awaitTermination(500, TimeUnit.MICROSECONDS)){
				continue;
			}
        	long estimatedTime = System.currentTimeMillis() - startTime;
        	
        	LogUtils.debug("\n",Unloader.class);
        	LogUtils.info("[SUMMARY_INFO]",Unloader.class);
        	
        	impSql = new File(ConfigInfo.OUTPUT_DIRECTORY + "data/import.sql");
        	PrintWriter pw = new PrintWriter(impSql);
        	
        	
        	StringBuffer sb = new StringBuffer(), impsb = new StringBuffer();
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
    				impsb.append("\\copy \"");
    				impsb.append(DevUtils.classifyString(jobList.get(i).getTableName(),ConfigInfo.CLASSIFY_STRING));
    				impsb.append("\" (");
    				for(int cnmIdx=0; cnmIdx < jobList.get(i).columnNames.size(); cnmIdx++ ) {
    					impsb.append("\"");
    					impsb.append(DevUtils.classifyString(jobList.get(i).columnNames.get(cnmIdx),ConfigInfo.CLASSIFY_STRING));
    					impsb.append("\"");
    					if(cnmIdx != jobList.get(i).columnNames.size()-1) {
    						impsb.append(", ");
    					}
    				}
    				impsb.append(") from '");
    				impsb.append(DevUtils.classifyString(jobList.get(i).getTableName(),ConfigInfo.CLASSIFY_STRING));
    				impsb.append(".out'\n");
    			} else {
    				sb.append("FAILURE");
    				failCnt++;
    			}
//    			sb.append('\n');
    			LogUtils.info(sb.toString(),Unloader.class);
    		}
    		pw.println(impsb);
    		impsb.setLength(0);
    		impsb = null;
    		pw.close();
    		
    		LogUtils.info(String.format("[TOTAL_INFO] SUCCESS : %d / FAILURE : %d / TOTAL: %d",jobList.size()-failCnt,failCnt,jobList.size()),Unloader.class);
    		LogUtils.info("[ELAPSED_TIME] " + makeElapsedTimeString(estimatedTime/1000),Unloader.class);
    		
    		//SUMMARY 파일 생성	   	
        	makeSummaryFile(jobList, estimatedTime);
    		//(new UnloadSummary("out/result", "summary")).run();
		}catch(Exception e){
			LogUtils.error("EXCEPTION!!!!",Unloader.class,e);
			System.exit(Constant.ERR_CD.UNKNOWN_ERR);
		} finally {
			if(executorService != null) executorService.shutdown();
		}
	}
	
	
	
	
	
	
	private void makeSummaryFile(List<ExecuteDataTransfer> jobList, long estimatedTime) {
		LogUtils.debug("\n",Unloader.class);
		LogUtils.debug("[MAKE_SUMMARY_FILE_START]",UnloadSummary.class);
		
		Calendar calendar = Calendar.getInstance();
        java.util.Date date = calendar.getTime();
        String today = (new SimpleDateFormat("yyyyMMddHHmmss").format(date));

		try {
			ByteBuffer fileBuffer = ByteBuffer.allocateDirect(ConfigInfo.BUFFER_SIZE);
			FileChannel fch = null;
				
			File file = new File(ConfigInfo.OUTPUT_DIRECTORY+"result/summary_"+today+".out");
			
			FileOutputStream fos = new FileOutputStream( file);
			fch = fos.getChannel();
			int failCnt = 0;
			
			for(int i=0;i<jobList.size();i++) {
				fileBuffer.put(" TABLE_NAME : ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(jobList.get(i).getTableName().getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(" ROWNUM : ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(String.valueOf(jobList.get(i).getRowCnt()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(" STATE : ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				if(jobList.get(i).isSuccess()){
					fileBuffer.put("SUCCESS".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
    			} else {
    				fileBuffer.put("FAILURE".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
    				failCnt++;
    			}
				fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));			
				fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));		
				fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));	
    		}
			
			fileBuffer.put(" [TOTAL_INFO] ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(" SUCCESS : ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(jobList.size()-failCnt).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			fileBuffer.put(" FAILURE : ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(failCnt).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			fileBuffer.put(" TOTAL : ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(jobList.size()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			fileBuffer.put("ELAPSED_TIME : ".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(makeElapsedTimeString(estimatedTime/1000)).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			
			fileBuffer.flip();
			fch.write(fileBuffer);
			fileBuffer.clear();
			
			fch.close();
			fos.close();			
		} catch ( Exception e ) {
			LogUtils.error("[MAKE_SUMMARY_FILE_FAIL]",Unloader.class,e);
		} finally {
			LogUtils.debug("[MAKE_SUMMARY_FILE_END]",Unloader.class);
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
