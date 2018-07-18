package com.k4m.experdb.db2pg.db;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.k4m.experdb.db2pg.common.LogUtils;

/**
 * CODER : JAEWON LEE
 * CREATED_DATE : 2017. 07. 13.
 * MODIFIED_DATE : 2017. 07. 14.
 * BODY : XML File parsing for create prepared statement
 * */
public class QueryMaker {
	public static int MAXIMUM_INIT_FAIL_COUNT = 3;
	public static String QUERY_TYPE_SELECT="SELECT",QUERY_TYPE_INSERT="INSERT",QUERY_TYPE_UPDATE="UPDATE", QUERY_TYPE_DELETE="DELETE";
	private List<String> paramNames = new ArrayList<String>();
	private Map<String,List<Integer>> paramIndexes = new HashMap<String,List<Integer>>();
	private static Map<String, ConcurrentHashMap<String,HashMap<String,ArrayList<Query>>>> mapperMgmt 
		= new ConcurrentHashMap <String, ConcurrentHashMap<String,HashMap<String,ArrayList<Query>>>>();
	private String fileLocation; 
//	public sd
	
	public QueryMaker(String fileLocation) {
		while(mapperMgmt.get(fileLocation) == null) {
			try {
				init(fileLocation);
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		}
		this.fileLocation = fileLocation;
	}
	
	
	//region init
	/**
	 * Constant.DB_TYPE 클래스에 정의된 DB 종류 타입들을 db type으로 사용가능하다.
	 * */
	private void init(String fileLocation) {
		boolean complete = false;
		ConcurrentHashMap<String,HashMap<String,ArrayList<Query>>> typeMapper = null;
		if((typeMapper = mapperMgmt.get(fileLocation)) == null) {
			typeMapper = new ConcurrentHashMap<String,HashMap<String,ArrayList<Query>>>();
		}
		
		HashMap<String,ArrayList<Query>> mapper = null;
		for(int i=0;i<MAXIMUM_INIT_FAIL_COUNT && !complete;i++) { // failcnt
			try {
//				InputSource inSrc = new InputSource(new FileReader(fileLocation));
				InputSource inSrc = new InputSource(QueryMaker.class.getResourceAsStream(fileLocation));
				
				Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inSrc);
				XPath xpath = XPathFactory.newInstance().newXPath();
				String[] expressions = {"/mapper","/MAPPER"};
				for(String expression : expressions) {
					NodeList rootNodeList = (NodeList) xpath.compile(expression).evaluate(document, XPathConstants.NODESET);
					if(rootNodeList.getLength()>0) {
						NodeList mapperNodes = rootNodeList.item(0).getChildNodes();
						for(int j=0; j<mapperNodes.getLength();j++){
							Node dbNode = mapperNodes.item(j);
							if(dbNode.getNodeName().toUpperCase().equals("DB")){
								Node dbTypeNode = dbNode.getAttributes().getNamedItem("type");
								String dbType = dbTypeNode.getNodeValue();
								mapper = typeMapper.get(dbType);
								if(mapper == null) {
									mapper = new HashMap<String,ArrayList<Query>>();
									typeMapper.put(dbType, mapper);
								}
								NodeList dbNodes = dbNode.getChildNodes();
								for(int k=0; k<dbNodes.getLength(); k++) {
									Node element = dbNodes.item(k);
									if(element.getNodeType() == Node.ELEMENT_NODE) {
										if(isContainQueryType(element.getNodeName().toUpperCase())){
											Node node = element.getAttributes().getNamedItem("id");
											String key = node!=null?node.getNodeValue():null;
											node = element.getAttributes().getNamedItem("version");
											String version = node!=null?node.getNodeValue():null;
											String value = element.getFirstChild().getNodeValue().trim();
											Query query = new Query();
											try {
												query.version = Double.parseDouble(version);
											} catch(Exception e){
												query.version = 0;
											}
											query.sql = value; 
											value = value.lastIndexOf(";") == -1 ? value : value.substring(0,value.length()-1);
											if(mapper.get(key) == null) {
												ArrayList<Query> querys = new ArrayList<Query>();
												querys.add(query);
												mapper.put(key, querys);
											} else {
												ArrayList<Query> querys = mapper.get(key);
												querys.add(query);
												Collections.sort(querys, new QueryCompare());
											}
										}
									}
								}
							}
						}
						
						complete = true;
						break;
					} else {
						continue;
					}
				}
			} catch (Exception e) {
				LogUtils.error(e.getMessage(), QueryMaker.class);
			}
		}
		if(!complete) {
			LogUtils.error("[EXCEEDED_MAXIMUM_MAPPER INIT_FAIL_COUNT]", QueryMaker.class);
			return;
		}
		mapperMgmt.put(fileLocation, typeMapper);
	}
	private class Query {
		double version;
		String sql;
	}
	private class QueryCompare implements Comparator<Query> {
		@Override
		public int compare(Query o1, Query o2) {
			if(o1.version<o2.version) {
				return -1;
			} else if(o1.version>o2.version) {
				return 1;
			} else {
				return 0;
			}
		}
	}
	//endregion
	
	//region functions
	/** 
	 * case(priority) : equal(1), lower_case(2)
	 * */
	public String getQuery(String queryId,String dbType, double version) {
		for(Query query : mapperMgmt.get(fileLocation).get(dbType).get(queryId)){
			if(version==query.version) {
				return query.sql;
			} 
		}
		for(Query query : mapperMgmt.get(fileLocation).get(dbType).get(queryId)){
			if(version>query.version) {
				return query.sql;
			}
		}
		return null;
	}
	
	public String getQuery(String queryId,String dbType, Map<String,Object> params, double version) {
		String sql = getQuery(queryId,dbType,version);
		
		if(sql == null) return null;
		int start = 0, end = 0;
		
		while((start=sql.indexOf("${", end)) != -1 && (end=sql.indexOf("}",start)) != -1) {
			String paramName = sql.substring(start+2,end); 
			Object value = params.get(paramName);
			if(value != null) sql = replaceString(sql, value.toString(), start, end);
			end=start+1;
		}
		
		int position = 0; 
		start = 0; end = 0;
		while((start=sql.indexOf("#{", end)) != -1 && (end=sql.indexOf("}",start)) != -1) {
			String paramName = sql.substring(start+2,end); 
			paramNames.add(paramName);
			if(paramIndexes.get(paramName) == null) {
				List<Integer> paramIdxs = new ArrayList<Integer>();
				paramIndexes.put(paramName,paramIdxs);
				paramIdxs.add(++position);
			} else {
				paramIndexes.get(paramName).add(++position);
			}
			
//			paramIndexes.put(paramName, ++position);
			sql = replaceString(sql, "?", start, end);
			end=start+1;
		}
		return sql;
	}
	
	
	public PreparedStatement getPreparedStatement(String queryId,String dbType,Connection connection,double version) throws SQLException {
		PreparedStatement pStmt = connection.prepareStatement(getQuery(queryId,dbType,version));
		return pStmt;
	}
	
	
	public PreparedStatement getPreparedStatement(String queryId,String dbType,Map<String,Object> params,Connection connection,double version) throws SQLException {
		if(params == null) return getPreparedStatement(queryId, dbType, connection, version);
		String sql = getQuery(queryId,dbType,params,version);
		PreparedStatement pStmt = connection.prepareStatement(sql);
		for(String paramName : paramNames) {
			Object value = params.get(paramName);
			if(value!=null) {
				for(int idx : paramIndexes.get(paramName)) {
					pStmt.setObject(idx,value);
				}
			}
		}
		return pStmt;
	}
	
	public void setPreparedStatement(Map<String,Object> params, PreparedStatement pStmt) throws SQLException {
		for(String paramName : paramNames) {
			Object value = params.get(paramName);
			if(value!=null) {
				for(int idx : paramIndexes.get(paramName)) {
					pStmt.setObject(idx,value);
				}
			}
		}
	}
	
	private String replaceString(String originString, String changeString, int start, int end) {
		StringBuffer sb = new StringBuffer();
		sb.append(originString.substring(0,start));
		sb.append(changeString);
		sb.append(originString.substring(end+1));
		return sb.toString();
	}
	
	private static synchronized boolean isContainQueryType(String type) {
		if(type.equals(QUERY_TYPE_SELECT)) {
			return true;
		} else if(type.equals(QUERY_TYPE_INSERT)) {
			return true;
		} else if(type.equals(QUERY_TYPE_UPDATE)) {
			return true;
		} else if(type.equals(QUERY_TYPE_DELETE)) {
			return true;
		} else {
			return false;
		}
	}
	//endregion

}

