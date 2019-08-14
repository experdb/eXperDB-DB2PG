package com.k4m.experdb.db2pg.db;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;

import com.k4m.experdb.db2pg.common.CommonUtil;
import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.mapper.TestMapper;

public class DBCPPoolManager {
	private DBCPPoolManager(){}
	public static ConcurrentHashMap<String, PoolInfo> connInfoList = new ConcurrentHashMap<String, PoolInfo>();
	
	private static SqlSessionFactoryBuilder sqlSessionFactoryBuilder = new SqlSessionFactoryBuilder();
	
	private static class PoolInfo {
		DBConfigInfo configInfo;
		SqlSessionFactory sqlSessionFactory;
		private PoolInfo(DBConfigInfo configInfo,SqlSessionFactory sqlSessionFactory) {
			this.configInfo = configInfo;
			this.sqlSessionFactory = sqlSessionFactory;
		}
	}
	
	public static void setupDriver(DBConfigInfo configInfo, String poolName, int maxActive) throws Exception {
		LogUtils.info("************************************************************",DBCPPoolManager.class);
		LogUtils.info("Create DBCPPOOL ["+poolName+"]",DBCPPoolManager.class);
		
		// JDBC 클래스 로딩
		try {
			String driver = "";
	        // DB URI
	        String connectURI = "";
			
	        Properties props = new Properties();
	        
    		switch (configInfo.DB_TYPE) {
				case Constant.DB_TYPE.ORA :
					driver = "oracle.jdbc.driver.OracleDriver";
					connectURI = "jdbc:oracle:thin:@"+configInfo.SERVERIP+":"+configInfo.PORT+"/"+configInfo.DBNAME;
					break;
				case Constant.DB_TYPE.POG :
					driver = "org.postgresql.Driver" ;
					connectURI = "jdbc:postgresql://"+configInfo.SERVERIP+":"+configInfo.PORT+"/"+configInfo.DBNAME;
					if(configInfo.SCHEMA_NAME != null && !configInfo.SCHEMA_NAME.equals("")) {
						connectURI += "?currentSchema="+configInfo.SCHEMA_NAME;
					}
					break;
				case Constant.DB_TYPE.ASE :
					driver = "com.sybase.jdbc4.jdbc.SybDriver" ;
					connectURI = "jdbc:sybase:Tds:"+configInfo.SERVERIP+":"+configInfo.PORT+"/"+configInfo.DBNAME;
					
					if (configInfo.LOAD_MODE != null && configInfo.LOAD_MODE.equals(Constant.DIRECT_PATH_LOAD)){
						props.put("ENABLE_BULK_LOAD ", "ARRAYINSERT_WITH_MIXED_STATEMENTS");
						LogUtils.info("PROPERTY : ENABLE_BULK_LOAD=ARRAYINSERT_WITH_MIXED_STATEMENTS",DBCPPoolManager.class);
					}
					break;
				case Constant.DB_TYPE.TBR :
					driver = "com.tmax.tibero.jdbc.TbDriver";
					connectURI = "jdbc:tibero:thin:@"+configInfo.SERVERIP+":"+configInfo.PORT+":"+configInfo.DBNAME;
					break;
				case Constant.DB_TYPE.MSS :
					driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver" ;
					connectURI = "jdbc:sqlserver://"+configInfo.SERVERIP+":"+configInfo.PORT+";databaseName="+configInfo.DBNAME;
					break;
				case Constant.DB_TYPE.DB2 :
					driver = "com.ibm.db2.jcc.DB2Driver" ;
					connectURI = "jdbc:db2://"+configInfo.SERVERIP+":"+configInfo.PORT+"/"+configInfo.DBNAME;
					System.setProperty("db2.jcc.charsetDecoderEncoder", "3");
					break;
				case Constant.DB_TYPE.MYS :
					driver = "com.mysql.jdbc.Driver" ;
					// mysql jdbc의 ResultSet fetch size 설정을 활성화 하려면 아래의 useCursorFetch 설정을 true로 변경하여야 한다.
					connectURI = "jdbc:mysql://"+configInfo.SERVERIP+":"+configInfo.PORT+"/"+configInfo.DBNAME+"?useCursorFetch=true";
					break;
				case Constant.DB_TYPE.CUB :
					driver = "cubrid.jdbc.driver.CUBRIDDriver" ;
					connectURI = "jdbc:CUBRID:"+configInfo.SERVERIP+":"+configInfo.PORT+":"+configInfo.DBNAME+":"+configInfo.DBNAME+"::";
					break;
    		}
    		
			Class.forName(driver);
			
			//DB 연결대기 시간
			DriverManager.setLoginTimeout(5);
			
	        // ID and Password
	        props.put("user", configInfo.USERID);
	        props.put("password", configInfo.DB_PW);
	        
	        if (configInfo.CHARSET != null){
	        	props.put("charset", configInfo.CHARSET);
	        }
	        LogUtils.info("PROPERTY : charset=" + configInfo.CHARSET,DBCPPoolManager.class);
	        
	        // 풀이 커넥션을 생성하는데 사용하는 DriverManagerConnectionFactory를 생성
	        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectURI, props);
	        
	        // ConnectionFactory의 래퍼 클래스인 PoolableConnectionFactory를 생성
	        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);	        
	        
	        // 커넥션 풀로 사용할 commons-collections의 genericOjbectPool을 생성 
	        GenericObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<PoolableConnection>(poolableConnectionFactory);
	        
	        // Pool에서 Connection을 받아와 DB에 Query문을 날리기 전에
	        // 해당 Connection이 Active한지 Check하고 
	        // Active하지 않으면 해당 Connection을 다시 생성합니다
	        //CUBRID는 setTestOnBorrow 속성 시에 Cannot get a connection, pool error: Unable to validate object 에러 발생
	        
	        if (!configInfo.DB_TYPE.equals("CUB")) {
		        connectionPool.setTestOnBorrow(true);	
	        }
	        
	        connectionPool.setTestOnReturn(true);
	        connectionPool.setTestWhileIdle(true);
	        connectionPool.setMaxTotal(maxActive);		        
	        connectionPool.setMaxWaitMillis(300000);  //사용할 커넥션이 없을 때 무한 대기
	        connectionPool.setMinEvictableIdleTimeMillis(30 * 1000);
	        connectionPool.setTimeBetweenEvictionRunsMillis(30 * 1000);
	        
	        poolableConnectionFactory.setPool(connectionPool);	        
	        
            //PoolingDriver 자신을 로딩
            Class.forName("org.apache.commons.dbcp2.PoolingDriver");
            PoolingDriver pDriver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
            PoolingDataSource<PoolableConnection> dataSource=  new PoolingDataSource<PoolableConnection> (connectionPool);
            dataSource.setAccessToUnderlyingConnectionAllowed(true);
            
            Environment env = new Environment(poolName, (TransactionFactory)new ManagedTransactionFactory(), dataSource);
			Configuration config = new Configuration(env); 
			config.setDatabaseId(configInfo.DB_TYPE);
			new XMLMapperBuilder(DBCPPoolManager.class.getResourceAsStream("/mapper/TestMapper.xml"),config,"TestMapper",config.getSqlFragments()).parse();
			new XMLMapperBuilder(DBCPPoolManager.class.getResourceAsStream("/mapper/MetaExtractMapper.xml"),config,"MetadataExtractMapper",config.getSqlFragments()).parse();
			config.setEnvironment(env);
			SqlSessionFactory sqlSessionFactory = sqlSessionFactoryBuilder.build(config);
            //Pool 등록
            pDriver.registerPool(poolName, connectionPool);
            
            connInfoList.put(poolName, new PoolInfo(configInfo,sqlSessionFactory));
            
            setDBConnInfo(poolName, configInfo);
            
		} catch (Exception e) {
			shutdownDriver(poolName);
			throw e;
		} finally {
			LogUtils.info("Created DBCPPOOL ["+poolName+"]",DBCPPoolManager.class);
			LogUtils.info("************************************************************",DBCPPoolManager.class);
		}
	}
	
	/**
	 * DB 정보 setting
	 * @param poolName
	 * @param configInfo
	 * @throws Exception
	 */
	private static void setDBConnInfo(String poolName, DBConfigInfo configInfo) throws Exception{
		Connection conn = getConnection(poolName);
		try {
        conn.setAutoCommit(false);
        configInfo.DB_VER = conn.getMetaData().getDatabaseMajorVersion() + "." + conn.getMetaData().getDatabaseMinorVersion();
        configInfo.DB_MAJOR_VER = conn.getMetaData().getDatabaseMajorVersion();
        configInfo.DB_MINOR_VER = conn.getMetaData().getDatabaseMinorVersion();
        configInfo.ORG_SCHEMA_NM= conn.getMetaData().getUserName();
		} catch(Exception e) {
			
		} finally {
	        if (conn != null) conn.close();
		}

	}
	
	public static void shutdownDriver(String poolName) throws Exception {
		PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
		driver.closePool(poolName);
		
		if (connInfoList.containsKey(poolName)) {
			connInfoList.remove(poolName);
		}		
	}
	
	public static void shutdownAllDriver() {
		try {
			PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
			for (String poolName : driver.getPoolNames()) {
				driver.closePool(poolName);
				if (connInfoList.containsKey(poolName)) {
					connInfoList.remove(poolName);
				}	
			}
		}catch(Exception e){
			LogUtils.error(CommonUtil.getStackTrace(e), DBCPPoolManager.class);
		}
	}
	
	/*
    public static void printDriverStats(String poolName) throws SQLException {
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
        ObjectPool connectionPool = driver.getConnectionPool(poolName);
    }
    */
	
    public static Connection getConnection(String poolName) throws Exception {
    	Connection conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:" + poolName);
    	/*
    	switch (getConfigInfo(poolName).DB_TYPE){
    		case Constant.DB_TYPE.ASE :
    			conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    	}
    	*/
    	conn.setAutoCommit(false);
    	
    	return conn;
    }
    
    public static SqlSession getSession(String poolName) throws Exception {
		return connInfoList.get(poolName).sqlSessionFactory.openSession(false);
	}
    
    public static DBConfigInfo getConfigInfo(String poolName) throws Exception {
    	if (connInfoList.containsKey(poolName)){
    		return connInfoList.get(poolName).configInfo;
    	}else{
    		return null;
    	}
    }
    
    public static boolean ContaintPool(String poolName) throws Exception {
    	if (connInfoList.containsKey(poolName)){
    		return true;    		
    	}else{
    		return false;
    	}
    }
    
    public static String[] GetPoolNameList() throws Exception {
    	PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
    	return driver.getPoolNames();
    }
    
    public static int getPoolCount() {
    	return connInfoList.size(); 
    }
}
