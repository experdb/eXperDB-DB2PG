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
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.convert.db.ConvertDBUtils;
import com.k4m.experdb.db2pg.convert.table.Column;
import com.k4m.experdb.db2pg.convert.table.Table;
import com.k4m.experdb.db2pg.db.DBUtils;

public class Unloader {
	static MsgCode msgCode = new MsgCode();
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
		
		List<String>  tableNameList = ConfigInfo.SRC_INCLUDE_TABLES;
		
		if(tableNameList == null){
			tableNameList = DBUtils.getTableNames(ConfigInfo.SRC_TABLE_DDL,Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_DB_CONFIG);
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
	
	// getConvertReplaceTableName에서 getConvertObjectName로 메소드명 변경
	private String getConvertObjectName(String objName) throws Exception {
		String strReplaceTableName = "";
		
		if(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.equals(Constant.DB_TYPE.MYS)) {
			strReplaceTableName = "`" + objName + "`";
		} else {
			strReplaceTableName =  "\"" + objName + "\"";
		}
		
		return strReplaceTableName;
	}
	
	private String getWhere() throws Exception {
		String strWhere ="";
		
		if(ConfigInfo.SRC_WHERE_CONDITION != null && !ConfigInfo.SRC_WHERE_CONDITION.equals("")) {
			strWhere = "WHERE "+ConfigInfo.SRC_WHERE_CONDITION;
		}
		return strWhere;
	}
	
	private void setSchemaNameCheck() throws Exception {
		if (ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME == null || ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.trim().equals("")) {
			ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME = ConfigInfo.SRC_DB_CONFIG.USERID;
		}
	}
	
	@SuppressWarnings("null")
	public void start() {
		
		ExecutorService executorService = Executors.newFixedThreadPool(ConfigInfo.SRC_SELECT_ON_PARALLEL);
		
		try {

			if (ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME==null && ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.equals("")) {
				LogUtils.error("SCHEMA_NAME NOT FOUND", Unloader.class);
				System.exit(0);
			}
			

			// Target DB Charset different vs config Charset
//			if(ConfigInfo.DB_WRITER_MODE && ConfigInfo.SRC_INCLUDE_DATA_EXPORT) {
//				String pgCharSet = getPgCharSet();
//				if(pgCharSet != null && !pgCharSet.toUpperCase().equals(ConfigInfo.TAR_DB_CONFIG.CHARSET.toUpperCase())) {
//					LogUtils.error("Target Database Charset is "+pgCharSet +". ", Unloader.class);
//					System.exit(Constant.ERR_CD.FAIL_CHARSET);
//				}
//			}
			
			startTime = System.currentTimeMillis();
			
			LogUtils.debug(msgCode.getCode("C0144"), Unloader.class);

			// Source Schema name 미입력 체크 
			setSchemaNameCheck();
			
			//DBCPPoolManager.setupDriver(ConfigInfo.SRC_DB_CONFIG, Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_SELECT_ON_PARALLEL);

			List<String> selSqlList = new ArrayList<String>();
			List<String> tableNameList =  null;
			int jobSize = 0;
			
			// Query XML file Check
			if(!ConfigInfo.SRC_FILE_QUERY_DIR_PATH.equals("")) {
				File f = new File(ConfigInfo.SRC_FILE_QUERY_DIR_PATH);
				if(f.exists() && !f.isDirectory())
					loadSelectQuery(ConfigInfo.SRC_FILE_QUERY_DIR_PATH);
			}

			// ASIS Data Export
			if(ConfigInfo.SRC_INCLUDE_DATA_EXPORT) {
				
				//Table Select
				tableNameList = makeTableList();
				for (String tableName : tableNameList) {
					Table table = new Table();

					String schema = ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME!=null && !ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.equals("") 
										? getConvertObjectName(ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME)+"." 
										: "" ;

					table.setSchemaName(ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME);
					table.setName(tableName);
					ConvertDBUtils.checkColumnInform(table, Constant.POOLNAME.SOURCE.name(), ConfigInfo.SRC_DB_CONFIG);
					
					String replaceTableName = getConvertObjectName(tableName);
					String where = getWhere();
					if(table.isCheckColumn()) {
						String sql = "SELECT ";
						List<Column> columns = table.getColumns();
						int i = 1;
						for(Column column : columns) {
							if( ((String)column.getType()).contains("XMLTYPE") ) {
								sql += "XMLSERIALIZE(CONTENT "+column.getName()+")";
							}else {
								sql += column.getName();
							}
							
							if(i < columns.size()) {
								sql += ",";
							}else {
								i++;
							}
						}
						selSqlList.add(String.format(sql+" FROM %s%s %s", schema, replaceTableName, where));
					}else {
						selSqlList.add(String.format("SELECT * FROM %s%s %s", schema, replaceTableName, where));
					}
				}

				if(selSqlList != null) {
					jobSize += selSqlList.size();
				}
				System.out.println(selSqlList.toString());
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
        	LogUtils.info(msgCode.getCode("C0145"),Unloader.class);
        	
        	// import.sql을 생성
        	impSql = new File(ConfigInfo.SRC_FILE_OUTPUT_PATH + "data/import.sql");
        	PrintWriter pw = new PrintWriter(impSql);
        	
        	
        	StringBuffer sb = new StringBuffer(), impsb = new StringBuffer();
        	int failCnt = 0;
    		for(int i=0;i<jobList.size();i++) {
    			sb.setLength(0);
    			sb.append("TABLE_NAME : ");
    			sb.append(jobList.get(i).getTableName());
    			sb.append(", ROWNUM : ");
    			sb.append(String.valueOf(jobList.get(i).getRowCnt()));
    			sb.append(", MIGTIME : ");
    			sb.append(jobList.get(i).getMigTime());
    			sb.append("ms, STATE : ");
    			if(jobList.get(i).isSuccess()){
    				sb.append("SUCCESS");
    				// 파일로 데이터 추출시 import.sql 파일에 psql을 이용한 데이터 적재가 가능하도록 구문 생성
    				// linux-ex) nohup psql -d testdb -U test -f import.sql > import.log 2>&1 &
    				impsb.append("\\copy \"");
    				impsb.append(DevUtils.classifyString(jobList.get(i).getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
    				impsb.append("\" (");
    				for(int cnmIdx=0; cnmIdx < jobList.get(i).columnNames.size(); cnmIdx++ ) {
    					impsb.append("\"");
    					impsb.append(DevUtils.classifyString(jobList.get(i).columnNames.get(cnmIdx),ConfigInfo.SRC_CLASSIFY_STRING));
    					impsb.append("\"");
    					if(cnmIdx != jobList.get(i).columnNames.size()-1) {
    						impsb.append(", ");
    					}
    				}
    				impsb.append(") from '");
    				impsb.append(DevUtils.classifyString(jobList.get(i).getTableName(),ConfigInfo.SRC_CLASSIFY_STRING));
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
    		
    		LogUtils.info(String.format(msgCode.getCode("C0146"),jobList.size()-failCnt,failCnt,jobList.size()),Unloader.class);
    		LogUtils.info(String.format(msgCode.getCode("C0147"),makeElapsedTimeString(estimatedTime/1000)),Unloader.class);
    		
    		//SUMMARY 파일 생성	   	
        	makeSummaryFile(jobList, estimatedTime);
    		//(new UnloadSummary("out/result", "summary")).run();
		}catch(Exception e){
			LogUtils.error(msgCode.getCode("C0148")+ " : "+ e,Unloader.class,e);
			System.exit(Constant.ERR_CD.UNKNOWN_ERR);
		} finally {
			if(executorService != null) executorService.shutdown();
		}
	}

	private void makeSummaryFile(List<ExecuteDataTransfer> jobList, long estimatedTime) {
		LogUtils.debug("\n",Unloader.class);
		LogUtils.debug(msgCode.getCode("C0149"),UnloadSummary.class);
		
		Calendar calendar = Calendar.getInstance();
        java.util.Date date = calendar.getTime();
        String today = (new SimpleDateFormat("yyyyMMddHHmmss").format(date));

		try {
			ByteBuffer fileBuffer = ByteBuffer.allocateDirect(ConfigInfo.SRC_BUFFER_SIZE);
			FileChannel fch = null;
				
			File file = new File(ConfigInfo.SRC_FILE_OUTPUT_PATH+"result/summary_"+ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME+"_"+today+".txt");
			
			FileOutputStream fos = new FileOutputStream( file);
			fch = fos.getChannel();
			int failCnt = 0;
			fileBuffer.put("Schema:TableName:Rownum:Migtime:State\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			for(int i=0;i<jobList.size();i++) {
				fileBuffer.put(ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME.getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(":".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(jobList.get(i).getTableName().getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(":".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(String.valueOf(jobList.get(i).getRowCnt()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(":".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(String.valueOf(jobList.get(i).getMigTime()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				fileBuffer.put(":".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
				if(jobList.get(i).isSuccess()){
					fileBuffer.put("SUCCESS".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
    			} else {
    				fileBuffer.put("FAILURE".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
    				failCnt++;
    			}
				fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
    		}
			
			fileBuffer.put(msgCode.getCode("C0150").getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(msgCode.getCode("C0151").getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(jobList.size()-failCnt).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			fileBuffer.put(msgCode.getCode("C0152").getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(failCnt).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			fileBuffer.put(msgCode.getCode("C0153").getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(jobList.size()).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put("\n".getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			fileBuffer.put(msgCode.getCode("C0154").getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			fileBuffer.put(String.valueOf(makeElapsedTimeString(estimatedTime/1000)).getBytes(ConfigInfo.TAR_DB_CONFIG.CHARSET));
			
			fileBuffer.flip();
			fch.write(fileBuffer);
			fileBuffer.clear();
			
			fch.close();
			fos.close();			
		} catch ( Exception e ) {
			LogUtils.error(msgCode.getCode("C0155"),Unloader.class,e);
		} finally {
			LogUtils.debug(msgCode.getCode("C0156"),Unloader.class);
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
		LogUtils.debug(msgCode.getCode("C0157"),Unloader.class);
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
				LogUtils.debug(msgCode.getCode("C0158"),Unloader.class);
			} else {
				LogUtils.warn(msgCode.getCode("C0159"),Unloader.class);
			}
		} catch ( Exception e ) {
			LogUtils.error(msgCode.getCode("C0160"),Unloader.class,e);
		} finally {
			LogUtils.debug(msgCode.getCode("C0161"),Unloader.class);
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
	
	
	/**
	 * PG CharSet 조회
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	private String getPgCharSet() throws Exception {
		String pgCharSet = null;
		
		pgCharSet = DBUtils.getCharSet(Constant.POOLNAME.TARGET.name(), ConfigInfo.TAR_DB_CONFIG);
		
		return pgCharSet;
	}
	
}
