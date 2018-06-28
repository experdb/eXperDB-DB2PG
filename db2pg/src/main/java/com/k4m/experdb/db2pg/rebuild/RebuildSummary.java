package com.k4m.experdb.db2pg.rebuild;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.common.StrUtil;

public class RebuildSummary {
	private File[] logFiles = null;
	private File summaryFile = null;
	private String outputDirectory = null;
	
	public RebuildSummary(String outputDirectory,String... logFileNames){
		summaryFile = new File("rebuild.summary");
		logFiles = new File[logFileNames.length];
		for(int i=0;i<logFileNames.length;i++){
			logFiles[i] = new File(logFileNames[i]);
		}
		this.outputDirectory= outputDirectory.trim().lastIndexOf("/")==outputDirectory.trim().length()-1 
				? outputDirectory.trim()
				: outputDirectory.trim()+"/";
	}
//	public static void main(String... args) {
//		(new RebuildSummary(".","rebuild.log")).run();
//	}
	public void run() {
		LogUtils.info("[MAKE_SUMMARY_FILE_START]",RebuildSummary.class);
		try {
			long allElapsedTime = 0;
			int fkCS = 0,fkCF = 0,fkDS = 0,fkDF = 0,idxCS = 0 ,idxCF = 0,idxDS = 0,idxDF = 0,unknown = 0;
			FileOutputStream fos = new FileOutputStream(outputDirectory+summaryFile);
			for ( int i=0;i<logFiles.length;i++){
				BufferedReader br = new BufferedReader(new FileReader(logFiles[i]));
				
				boolean isStart = false;
				String msg = null;
				List<LogInformation> infoList = new ArrayList<LogInformation>();
				LogInformation currInfo = null;
				LogUtils.info("[RUN_PARSING] "+ logFiles[i],RebuildSummary.class);
				int line = 0;
				try {
					int start, end;
					FileStyle fileStyle = FileStyle.unknown;
					while((msg = br.readLine())!= null) {
						line++;
						if(msg.startsWith("[FILE_NAME]")) {
							//fk_drop("fk_drop"),idx_drop("idx_drop"),idx_create("idx_create"),fk_create("fk_create");
							
							String filename = msg.substring(msg.indexOf(" ")+1);
							switch(filename) {
							case "fk_drop":
								fileStyle = FileStyle.fk_drop;
								break;
							case "idx_drop":
								fileStyle = FileStyle.idx_drop;
								break;
							case "idx_create":
								fileStyle = FileStyle.idx_create;
								break;
							case "fk_create":
								fileStyle = FileStyle.fk_create;
								break;
							default :
								fileStyle = FileStyle.unknown; 
							}
							
						} else if(msg.startsWith("[SQL]")) {
							if(currInfo != null) {
								infoList.add(currInfo);
								LogUtils.info(currInfo.toString(),RebuildSummary.class);
								fos.write(currInfo.toString().getBytes());
								fos.write('\n');
							}
							currInfo = new LogInformation(fileStyle);
							isStart = true;
							currInfo.sql = msg.substring(msg.indexOf('"')+1, msg.lastIndexOf('"'));
						} else if (StrUtil.PSQL_TIMING_PATTERN.matcher(msg).matches()) {
							if((start = msg.indexOf(":")) != -1  && (end = msg.indexOf("ms")) != -1){
								currInfo.elapsedTime += Long.valueOf(msg.substring(start+1,end).trim().replace(".", ""));
							}
						} else {
							if(isStart) {
								if (msg.startsWith("ALTER") || msg.startsWith("DROP") || msg.startsWith("CREATE")) {
									currInfo.isSuccess = true;
									isStart = false;
								}
							} 
						}
					}
					if(currInfo != null) {
						infoList.add(currInfo); 
						LogUtils.info(currInfo.toString(),RebuildSummary.class);
						fos.write(currInfo.toString().getBytes());
						fos.write('\n');
					}
				} catch( Exception e) {
					LogUtils.error(line+"line ",RebuildSummary.class,e);
				}
				
				LogUtils.info("[END_PARSING] "+ logFiles[i],RebuildSummary.class);
				
				for ( LogInformation info : infoList) {
					switch(info.style) {
					case fk_drop:
						if(info.isSuccess) fkDS++;
						else  fkDF++;
						break;
					case idx_drop:
						if(info.isSuccess) idxDS++;
						else  idxDF++;
						break;
					case idx_create:
						if(info.isSuccess) idxCS++;
						else  idxCF++;
						break;
					case fk_create:
						if(info.isSuccess) fkCS++;
						else  fkCF++;
						break;
					case unknown:
						unknown++;
						break;
					}
					allElapsedTime += ((info.elapsedTime+500)/1000+500)/1000;
				}
				br.close();
				
			}
			String rStr = String.format("\nSUCCESS: FK_CREATE=%d IDX_CREATE=%d FK_DROP=%d IDX_DROP=%d\n"
					+ "FAILURE: FK_CREATE=%d IDX_CREATE=%d FK_DROP=%d IDX_DROP=%d\n"
					+ "UNKNOWN: UNKNOWN=%d\n"
					,fkCS,idxCS,fkDS,idxDS,fkCF,idxCF,fkDF,idxDF,unknown);
			LogUtils.info(rStr,RebuildSummary.class);
			fos.write( rStr.getBytes() );
			LogUtils.info("TOTAL_ELAPSED_TIME=\""+StrUtil.makeElapsedTimeString(allElapsedTime)+"\"",RebuildSummary.class);
			fos.write( ("\nTOTAL_ELAPSED_TIME=\""+StrUtil.makeElapsedTimeString(allElapsedTime)+"\"").getBytes() );
			fos.flush();
			
			fos.close();
			LogUtils.info("[MAKE_SUMMARY_FILE_SUCCESS]",RebuildSummary.class);
    	} catch (Exception e) {
    		LogUtils.error("[MAKE_SUMMARY_FILE_FAIL]",RebuildSummary.class,e);
		}
		LogUtils.info("[MAKE_SUMMARY_FILE_END]",RebuildSummary.class);
	}
	private enum FileStyle {
		fk_drop("fk_drop"),idx_drop("idx_drop"),idx_create("idx_create"),fk_create("fk_create"), unknown("unknown");
		FileStyle(String style) {
		}
	}
	private static class LogInformation {
		long elapsedTime;
		String sql;
		FileStyle style;
		boolean isSuccess = false;
		LogInformation(FileStyle style) {
			this.style = style;
		}
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("TYPE=\"");
			sb.append(style);
			sb.append("\", ");
			sb.append("SQL=\"");
			sb.append(sql);
			sb.append("\", ");
			sb.append("ELAPSED_TIME=\"");
			sb.append(StrUtil.makeElapsedTimeString(((elapsedTime+500)/1000+500)/1000));
			sb.append("\", ");
			sb.append(isSuccess?"SUCCESS":"FAILURE");
			return sb.toString();
		}
		
	}
}
