package com.k4m.experdb.db2pg.common;

public class Constant {
	private Constant(){}
	public static String S = System.getProperty("file.separator"); // system file separator : "/" on UNIX, "\\" on WINDOWS
	public static String R = System.getProperty("line.separator"); // "\n" on LINUX, "\r\n" on WINDOWS
	public static String L = System.getProperty("user.language");
    
    public static String REPOSITORY_DB_TYPE;
    
    public static final String CONVENTIONAL = "CONVENTIONAL";
    public static final String DIRECT_PATH_LOAD = "DIRECT PATH LOAD";
    
    public static final String SOURCE_POOL_ID = "SOURCE";   //소스 POOL을 식별하기 위한 스트링
    public static final String TARGET_POOL_ID = "TARGET";   //타겟 POOL을 식별하기 위한 스트링
        
    public static final String ORA_STAT_TABLE_NM = "DX$_$STAT"; //오라클  인덱스 통계정보 저장 테이블 명 

    //DB 종류 타입
    public class DB_TYPE
    {
    	public static final String ORA = "ORA";
    	public static final String POG = "POG";
	    public static final String MSS = "MSS";
	    public static final String TBR = "TBR";
	    public static final String DB2 = "DB2";
	    public static final String ASE = "ASE";
	    public static final String MYS = "MYS";
	    public static final String CUB = "CUB";
	    public static final String ALT = "ALT";
    }

    public static enum POOLNAME
    {
    	REPOSITORY, SOURCE, SOURCE_DDL, TARGET;
    }    

    public static final String SERVER_LOCAL_IP = "127.0.0.1";


    public class ERR_CD {
    	public static final int FAILED_CREATE_DIR_ERR = 550;
    	public static final int UNKNOWN_ERR = 500;
    	public static final int METHOD_NOT_ALLOWD_ERR = 405;
    	public static final int SUCCESS = 0;
    	public static final int CONFIG_NOT_FOUND = 406;
    }
    

}