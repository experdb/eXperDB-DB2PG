package test;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.mapper.TestMapper;

public class TestMapperTest {
	public static void main(String...args) throws Exception {
		
		DBConfigInfo dbconf = new DBConfigInfo();
		dbconf.SERVERIP="192.168.56.44";
		dbconf.PORT="5432";
		dbconf.DBNAME="postgres";
		dbconf.USERID="experdba";
		dbconf.DB_PW="experdba";
		dbconf.DB_TYPE=Constant.DB_TYPE.POG;
		String poolName = "MetaExtractWorkerTest";
		DBCPPoolManager.setupDriver(dbconf, poolName, 2);
		
		SqlSession sqlSession = DBCPPoolManager.getSession(poolName);
		TestMapper mapper = sqlSession.getMapper(TestMapper.class);
		
		List<Map<String,Object>> results = mapper.test();
		
		for(Map<String,Object> result : results) {
			int t3 = (Integer)result.get("t3");
			String t2  = (String)result.get("t2");
			System.out.println(t3);
			System.out.println(t2);
		}
		
		
		sqlSession.close();
		
	}
}
