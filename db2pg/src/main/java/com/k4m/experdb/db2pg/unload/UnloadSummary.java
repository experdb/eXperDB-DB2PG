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
import com.k4m.experdb.db2pg.config.MsgCode;

public class UnloadSummary {
	static MsgCode msgCode = new MsgCode();
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
		LogUtils.debug(msgCode.getCode("C0162"),UnloadSummary.class);
		try {
			BufferedReader br = new BufferedReader(new FileReader(logFile));
			FileOutputStream fos = new FileOutputStream(outputDirectory+summaryFile);
			String msg = null;
			List<ProcessedInfo> pInfos = new ArrayList<ProcessedInfo>(); 
			ProcessedInfo pInfo = null;
			LogUtils.debug(msgCode.getCode("C0163"),UnloadSummary.class);
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
			
			LogUtils.debug(msgCode.getCode("C0164"),UnloadSummary.class);
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
			LogUtils.info(String.format(msgCode.getCode("C0165"),tableCnt,tableCorrectCnt,tableIncorrectCnt,StrUtil.makeElapsedTimeString(allElapsedTime)),UnloadSummary.class);
			fos.write( ("\n"+String.format(msgCode.getCode("C0165"),tableCnt,tableCorrectCnt,tableIncorrectCnt,StrUtil.makeElapsedTimeString(allElapsedTime))).getBytes() );
			fos.flush();
			br.close();
			fos.close();
			LogUtils.debug(msgCode.getCode("C0166"),UnloadSummary.class);
    	} catch (Exception e) {
    		LogUtils.error(msgCode.getCode("C0167"),UnloadSummary.class,e);
		}
		LogUtils.debug(msgCode.getCode("C0168"),UnloadSummary.class);
	}
	private class ProcessedInfo {
		long selectCount, copyCount, elapsedTime;
		String tablename;
		boolean status;
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(String.format(msgCode.getCode("C0169"),tablename));
			sb.append(", ");
			sb.append(String.format(msgCode.getCode("C0170"),selectCount));
			sb.append(", ");
			sb.append(String.format(msgCode.getCode("C0171"),copyCount));
			sb.append(", ");
			sb.append(String.format(msgCode.getCode("C0172"),StrUtil.makeElapsedTimeString(((elapsedTime+500)/1000+500)/1000)));
			sb.append(", ");
			sb.append(String.format(msgCode.getCode("C0173"),status?"SUCCESS":"FAILURE"));
			return sb.toString();
		}
		
	}
}
