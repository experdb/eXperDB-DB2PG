package com.k4m.experdb.db2pg.unload;

import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.common.StrUtil;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.config.MsgCode;
import com.k4m.experdb.db2pg.convert.table.CustomSql;

public class UnloadReport {
	static MsgCode msgCode = new MsgCode();
	List<ExecuteDataTransfer> jobList;
	List<CustomSql> selectQuerys = new ArrayList<CustomSql>();
	StringBuffer sb = new StringBuffer(), impsb = new StringBuffer();
	long startTime;
	long endTime;
	int tblCnt;
	
	public UnloadReport(List<ExecuteDataTransfer> jobList,List<CustomSql> selectQuerys, long startTime, long endTime, int tblCnt) {
		this.jobList = jobList;
		this.startTime = startTime;
		this.endTime = endTime;
		this.selectQuerys = selectQuerys;
		this.tblCnt = tblCnt;
	}
	
	public void run() {
		Calendar calendar = Calendar.getInstance();
        java.util.Date date = calendar.getTime();
		String today = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
		
		sb.append("<html>\r\n" + 
				"<head>\r\n" + 
				"	<style>\r\n" + 
				"		table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;}\r\n" + 
				"		table tr td.value, table tr td.mono {font-family: Monospace;}\r\n" +
				"		table tr.child td {background-color: #BBDD97; border-top-style: hidden;}\r\n" + 
				"		table tr:nth-child(even) {background-color: #eee;}\r\n" + 
				"		table tr:nth-child(odd) {background-color: #fff;}\r\n" + 
				"		table th {color: black; background-color: #ffcc99;}\r\n" + 
				"		table tr:target td {background-color: #EBEDFF;}\r\n" + 
				"		table tr:target td:first-of-type {font-weight: bold;}\r\n" + 
				"	</style>\r\n" + 
				"	<title>eXperDB-DB2PG Migration Report</title>\r\n" + 
				"</head>\r\n" + 
				"<body>\r\n" + 
				"	<H1>eXperDB-DB2PG Migration Report</H1>\r\n" + 
				"	<p>Report Date : " + today +"</p>\r\n" + 
				"	<H2>Report sections</H2> 	<ul>\r\n" + 
				"		<li><a HREF=#db1>Source & Target Infomation</a></li>\r\n" + 
				"		<li><a HREF=#db2>Migration statistics</a></li>\r\n" + 
				"		<li><a HREF=#db3>Statements statistics by table</a></li>\r\n");
				
		if(selectQuerys.size() > 0) {
			sb.append("		<li><a HREF=#db4>Custom SQL</a></li>\r\n");
		}
		sb.append(" 	</ul>\r\n" + 
				" 	<H2><a NAME=db1>Source & Target Infomation</a></H2> 	<table>\r\n" + 
				" 		<tr>\r\n" + 
				" 			<th>Type</th>\r\n" + 
				" 			<th>DBMS Type</th>\r\n" + 
				" 			<th>Host</th>\r\n" + 
				" 			<th>User</th>\r\n" + 
				" 			<th>Database</th>\r\n" + 
				" 			<th>Schema</th>\r\n" + 
				" 			<th>Port</th>\r\n" + 
				" 			<th>CharSet</th>\r\n" + 
				" 		</tr>\r\n" + 
				" 		<tr>\r\n" + 
				" 			<td><b>Source</b></td>\r\n" + 
				" 			<td class=\"value\">"+ msgCode.getCode(ConfigInfo.SRC_DB_CONFIG.DB_TYPE) +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.SRC_DB_CONFIG.SERVERIP +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.SRC_DB_CONFIG.USERID +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.SRC_DB_CONFIG.DBNAME +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.SRC_DB_CONFIG.SCHEMA_NAME +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.SRC_DB_CONFIG.PORT +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.SRC_DB_CONFIG.CHARSET +"</td>\r\n" + 
				" 		</tr><tr>\r\n" + 
				" 			<td><b>Target</b></td>\r\n" + 
				" 			<td class=\"value\">"+ msgCode.getCode(ConfigInfo.TAR_DB_CONFIG.DB_TYPE) +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.TAR_DB_CONFIG.SERVERIP +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.TAR_DB_CONFIG.USERID +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.TAR_DB_CONFIG.DBNAME +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.TAR_DB_CONFIG.SCHEMA_NAME +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.TAR_DB_CONFIG.PORT +"</td>\r\n" + 
				" 			<td class=\"value\">"+ ConfigInfo.TAR_DB_CONFIG.CHARSET +"</td>\r\n" + 
				" 		</tr>\r\n" + 
				" 	</table>"
				+ "");
		
		if(jobList.size() > 0) {
			impsb.append(" 	<H2><a NAME=db3>Statements statistics by table</a></H2>\r\n" + 
					" 	<table>\r\n" + 
					" 		<tr>\r\n" + 
					" 			<th>No</th>\r\n" +
					" 			<th>Result</th>\r\n" +
					" 			<th>Target Table</th>\r\n" + 
					" 			<th>Start Date</th>\r\n" + 
					" 			<th>End Date</th>\r\n" + 
					" 			<th>Mig Time</th>\r\n" + 
					" 			<th>Mig Byte</th>\r\n" + 
					" 			<th>Mig Count</th>\r\n" + 
					" 			<th>Fail Count</th>\r\n" + 
					"		</tr>");
			long failCnt = 0;
			long successCnt = 0;
			long totalByte = 0;
			int no = 1;
			String result = "";
		
			SimpleDateFormat tf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String starttime, endtime, titleTag;			
			for(int i=0;i<jobList.size();i++) {
				starttime = endtime = titleTag = "";
				totalByte += (long)jobList.get(i).getProcessBytes();
				failCnt += (long)jobList.get(i).getProcessErrorLInes();
				successCnt += (long)jobList.get(i).getProcessLines();
				if(jobList.get(i).getStartTime() > 0) starttime = tf.format(jobList.get(i).getStartTime());
				if(jobList.get(i).getEndTime() > 0) endtime = tf.format(jobList.get(i).getEndTime());
				if(jobList.get(i).isSuccess()) { result = "Success"; }
				else {
					result = "Fail";
					titleTag = " title='None Exist Table in Target DB'";
				}
				impsb.append(" 		<tr"+titleTag+">\r\n" + 
						" 			<td align=center><b>"+ no +"</b></td>\r\n" +
						" 			<td align=center><b>"+ result +"</b></td>\r\n" +
						" 			<td class=\"value\">" + jobList.get(i).getTableName() + "</td>\r\n" + 
						" 			<td class=\"value\">" + starttime + "</td>\r\n" + 
						" 			<td class=\"value\">" + endtime + "</td>\r\n" + 
						" 			<td class=\"value\" align=right>" + StrUtil.makeElapsedTimeString(jobList.get(i).getMigTime()) + "</td>\r\n" + 
						" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(jobList.get(i).getProcessBytes())) + "</td>\r\n" + 
						" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(jobList.get(i).getProcessLines())) + "</td>\r\n" + 
						" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(jobList.get(i).getProcessErrorLInes())) + "</td>\r\n" + 
						" 		</tr>\r\n");
				no++;
			}
			impsb.append(" 	</table>\r\n");
			
			String startDate = tf.format(startTime);
			String endDate = tf.format(endTime);
			Long st = (endTime-startTime)/1000;
			Long mig_sp1, mig_sp2;
			if(st>0) {
				mig_sp1 = totalByte/st;
				mig_sp2 = successCnt/st;
			}else {
				mig_sp1 = totalByte;
				mig_sp2 = successCnt;
			}
			
			sb.append(" 	<H2><a NAME=db2>Migration statistics</a></H2>\r\n" +
					"	<table>\r\n" + 
					" 		<tr>\r\n" + 
					" 			<th>Database</th>\r\n" + 
					" 			<th>Table Count</th>\r\n" + 
					" 			<th>Start Date</th>\r\n" + 
					" 			<th>End Date</th>\r\n" + 
					" 			<th>Total Time</th>\r\n" + 
					" 			<th>Mig Byte</th>\r\n" +
					" 			<th>Byte/Sec</th>\r\n" +					
					" 			<th>Success Count</th>\r\n" +
					" 			<th>Count/Sec</th>\r\n" +
					" 			<th>Fail Count</th>\r\n" + 
					" 		</tr>\r\n" + 
					" 		<tr>\r\n" + 
					" 			<td><b>" + ConfigInfo.TAR_DB_CONFIG.DBNAME + "</b></td>\r\n" + 
					" 			<td class=\"value\">" + StrUtil.strToComma(Integer.toString(jobList.size())) + "</td>\r\n" + 
					" 			<td class=\"value\">" + startDate + "</td>\r\n" + 
					" 			<td class=\"value\">" + endDate + "</td>\r\n" + 
					" 			<td class=\"value\">" + StrUtil.makeElapsedTimeString((endTime-startTime)/1000) + "</td>\r\n" + 
					" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(totalByte)) + "</td>\r\n" +
					" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(mig_sp1)) + "</td>\r\n" +					
					" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(successCnt)) + "</td>\r\n" +
					" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(mig_sp2)) + "</td>\r\n" +
					" 			<td class=\"value\" align=right>" + StrUtil.strToComma(Long.toString(failCnt)) + "</td>\r\n" + 
					" 		</tr>\r\n" + 
					" 	</table>");
			
			sb.append(impsb.toString());
		}
		
		if(selectQuerys.size() > 0) {
			sb.append(" 	<H2><a NAME=db4>Custom SQL</a></H2>\r\n" + 
					" 	<table>\r\n" + 
					" 		<tr>\r\n" + 
					" 			<th>SQL No.</th>\r\n" + 
					" 			<th>Target Table</th>\r\n" + 
					" 			<th>SQL</th>\r\n" + 
					" 		</tr>\r\n");
			
			for(int i=0;i<selectQuerys.size();i++) {
				sb.append(" 		<tr>\r\n" + 
						" 			<td class=\"value\" align=center><b>"+(i+1+tblCnt)+"</b></td>\r\n" + 
						" 			<td class=\"value\">" + selectQuerys.get(i).getName() + "</td>\r\n" + 
						" 			<td class=\"value\">" + selectQuerys.get(i).getQuery().replace("\n", "\n</br>") + "</td>\r\n" + 
						" 		</tr>\r\n");
			}
			
			sb.append(" 	</table>\r\n");
		}

		sb.append("</body>\r\n" + 
				"</html>");
		
		try {
			FileOutputStream fos = new FileOutputStream(ConfigInfo.SRC_FILE_OUTPUT_PATH+"result/report.html");
			fos.write((sb.toString()+"\n").getBytes());
			fos.flush();
			fos.close();
		} catch ( Exception e ) {
			LogUtils.error(msgCode.getCode("C0174"),Unloader.class,e);
		}
	}
}
