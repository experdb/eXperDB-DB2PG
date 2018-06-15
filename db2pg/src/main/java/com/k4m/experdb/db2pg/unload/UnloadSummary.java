package com.k4m.experdb.db2pg.unload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.common.StrUtil;

public class UnloadSummary {
	private File logFile = null, summaryFile = null;
	private String outputDirectory = null;
	
//	public static void main(String[] args) {
//		(new Summary("unload.log", ".")).run();
//	}
	
	public UnloadSummary(String outputDirectory, String logFileName) {
		if(logFileName != null) {
			this.logFile = new File(logFileName);
		} else {
			this.logFile = new File("unload.log");
		}
		this.summaryFile = new File("unload.summary");
		this.outputDirectory= outputDirectory.trim().lastIndexOf("/")==outputDirectory.trim().length()-1 
				? outputDirectory.trim()
				: outputDirectory.trim()+"/";
	}
	
	public void run() {
		LogUtils.debug("[MAKE_SUMMARY_FILE_START]",UnloadSummary.class);
		try {
			BufferedReader br = new BufferedReader(new FileReader(logFile));
			FileOutputStream fos = new FileOutputStream(outputDirectory+summaryFile);
			String msg = null;
			List<ProcessedInfo> pInfos = new ArrayList<ProcessedInfo>(); 
			ProcessedInfo pInfo = null;
			LogUtils.debug("[RUN_PARSING]",UnloadSummary.class);
			int line = 0;
			try {
				int start, end;
				while((msg = br.readLine())!= null) {
					line++;
					if (msg.startsWith("TABLE_NAME(ROW_COUNT)")) {
						if(pInfo != null) {
							if(pInfo.selectCount == pInfo.copyCount) {
								pInfo.status = true;
							} else {
								pInfo.status = false;
							}
							pInfos.add(pInfo);
						}
						pInfo = new ProcessedInfo();
						pInfo.tablename = msg.substring(msg.indexOf(">>> ")+4,msg.lastIndexOf("("));
						pInfo.selectCount = Long.valueOf(msg.substring(msg.lastIndexOf("(")+1,msg.lastIndexOf(")")));
					} else if (StrUtil.PSQL_TIMING_PATTERN.matcher(msg).matches()) {
						if((start = msg.indexOf(":")) != -1  && (end = msg.indexOf("ms")) != -1){
							pInfo.elapsedTime += Long.valueOf(msg.substring(start+1,end).trim().replace(".", ""));
						}
					} else if (msg.startsWith("COPY")) {
						pInfo.copyCount += Long.valueOf(msg.substring(5).trim());
					}
				}
				if(pInfo != null) {
					if(pInfo.selectCount == pInfo.copyCount) {
						pInfo.status = true;
					} else {
						pInfo.status = false;
					}
					pInfos.add(pInfo);
				}
			} catch( Exception e) {
				LogUtils.error(Integer.toString(line),UnloadSummary.class,e);
			}
			
			LogUtils.debug("[END_PARSING]",UnloadSummary.class);
			long allElapsedTime = 0;
			int tableCnt = 0, tableCorrectCnt = 0,tableIncorrectCnt = 0;
			
			for ( ProcessedInfo info : pInfos) {
				fos.write((info.toString()+"\n").getBytes());
				LogUtils.info(info.toString(), UnloadSummary.class);
				allElapsedTime += ((info.elapsedTime+500)/1000+500)/1000;
				if(info.status) {
					tableCorrectCnt++;
				} else {
					tableIncorrectCnt++;
				}
				tableCnt++;
			}
			LogUtils.info("TOTAL="+tableCnt
					+ ", SUCCESS="+tableCorrectCnt
					+ ", FAILURE="+tableIncorrectCnt
					+ ", TOTAL_ELAPSED_TIME=\""+StrUtil.makeElapsedTimeString(allElapsedTime)+"\""
					,UnloadSummary.class);
			fos.write( ("\nTOTAL="+tableCnt
					+ ", SUCCESS="+tableCorrectCnt
					+ ", FAILURE="+tableIncorrectCnt
					+ ", TOTAL_ELAPSED_TIME=\""+StrUtil.makeElapsedTimeString(allElapsedTime)+"\"").getBytes() );
			fos.flush();
			br.close();
			fos.close();
			LogUtils.debug("[MAKE_SUMMARY_FILE_SUCCESS]",UnloadSummary.class);
    	} catch (Exception e) {
    		LogUtils.error("[MAKE_SUMMARY_FILE_FAIL]",UnloadSummary.class,e);
		}
		LogUtils.debug("[MAKE_SUMMARY_FILE_END]",UnloadSummary.class);
	}
	private class ProcessedInfo {
		long selectCount, copyCount, elapsedTime;
		String tablename;
		boolean status;
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("TABLENAME=");
			sb.append(tablename);
			sb.append(", ");
			sb.append("SELECT_COUNT=");
			sb.append(selectCount);
			sb.append(", ");
			sb.append("COPY_COUNT=");
			sb.append(copyCount);
			sb.append(", ");
			sb.append("ELAPSED_TIME=\"");
			sb.append(StrUtil.makeElapsedTimeString(((elapsedTime+500)/1000+500)/1000));
			sb.append('"');
			sb.append(", ");
			sb.append("STATUS=");
			sb.append(status?"SUCCESS":"FAILURE");
			return sb.toString();
		}
		
	}
}
