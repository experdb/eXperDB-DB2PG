package test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.db.DBCPPoolManager;
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;
import com.k4m.experdb.db2pg.work.impl.MetaExtractWork;
import com.k4m.experdb.db2pg.work.impl.MetaExtractWorker;

public class MetaExtractWorkerTest {
	public static void main(String[] args) throws Exception {
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		
		DBConfigInfo dbconf = new DBConfigInfo();
		dbconf.SERVERIP="192.168.56.44";
		dbconf.PORT="5432";
		dbconf.DBNAME="postgres";
		dbconf.USERID="experdba";
		dbconf.DB_PW="experdba";
		dbconf.DB_TYPE=Constant.DB_TYPE.POG;
		String poolName = "MetaExtractWorkerTest";
		DBCPPoolManager.setupDriver(dbconf, poolName, 2);
		List<MetaExtractWork> works = new LinkedList<MetaExtractWork>();
		
		works.add(new MetaExtractWork(MetaExtractWorker.WORK_TYPE.GET_PG_CURRENT_SCHEMA, null));
		MetaExtractWorker metaExtractWorker = new MetaExtractWorker(poolName, works);
		System.out.println(metaExtractWorker.getPoolName());
		
		System.out.println(metaExtractWorker.isRunning());
//		metaExtractWorker.run();
		executorService.execute(metaExtractWorker);
		Thread.sleep(30);
		System.out.println(metaExtractWorker.isRunning());
		Thread.sleep(10);
		System.out.println(metaExtractWorker.isRunning());
		
		System.out.println(metaExtractWorker.getResult());
		
		executorService.shutdown();
		while(!executorService.awaitTermination(100, TimeUnit.MICROSECONDS)){
			continue;
		}
	}
}
