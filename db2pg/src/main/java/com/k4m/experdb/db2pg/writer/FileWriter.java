package com.k4m.experdb.db2pg.writer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.config.MsgCode;

public class FileWriter {
	static MsgCode msgCode = new MsgCode();
	protected String outputDirectory = ConfigInfo.SRC_FILE_OUTPUT_PATH + "data/";
	private  FileChannel fileChannels;
	private  FileChannel badFileChannels ;
	private  FileChannel progressFileChannels ;
	private static long successByteCount; // Writer가 처리한 총 Byte 수
	private static long errByteCount = 0; // 
	public static ConcurrentHashMap<String, ConfigInfo> ConnInfoList = new ConcurrentHashMap<String, ConfigInfo>();
	
	public FileWriter(){}
	
	public FileWriter(String table_nm) throws IOException{
		table_nm = table_nm.replace("\"", "");
		fileCreater(outputDirectory + table_nm+ ".out");			
	}
	
	public boolean dataWriteToFile(String lineStr, String table_nm) throws Exception {
		table_nm = table_nm.replace("\"", "");
		byte[] inputBytes = (lineStr).getBytes();
		ByteBuffer byteBuffer = ByteBuffer.wrap(inputBytes);
		try {
			///if (lineStr.contains("시스템명")) {
			//	throw new Exception();
			//}
			fileChannels.write(byteBuffer);
			successByteCount++;
		} catch (Exception e) {
			// bad파일 생성
			badFileCreater(outputDirectory + table_nm + ".bad");
			badFileWrite(lineStr);
			e.printStackTrace();
			closeBadFileChannels();
			return false;
		}
		return true;
	}
	

	public void badFileWrite(String lineStr) throws IOException {
		// Constant.R : LINUX = '\n', WINDOWS = '\r\n'
		byte[] inputBytes = (lineStr + Constant.R).getBytes();
		ByteBuffer byteBuffer = ByteBuffer.wrap(inputBytes);
		try {
			badFileChannels.write(byteBuffer);
			errByteCount++;
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public void fileCreater(String file_nm) throws IOException {
		File file = new File(file_nm);
		if(ConfigInfo.TAR_FILE_APPEND && file.isFile()) {
			fileChannels = FileChannel.open(file.toPath(), StandardOpenOption.APPEND);
		} else {
			fileChannels = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		}
	}

	public void badFileCreater(String file_nm) throws IOException {
		File file = new File(file_nm);
		if(file.isFile()) {
			badFileChannels = FileChannel.open(file.toPath(), StandardOpenOption.APPEND);
		} else {
			badFileChannels = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		}
	}
	
	public void progressFile(String file_nm) throws IOException {
		File file = new File(file_nm);
		if(file.isFile()) {
			progressFileChannels = FileChannel.open(file.toPath(), StandardOpenOption.APPEND);
		} else {
			progressFileChannels = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		}
	}
	
	public void progressFileWrite(String lineStr) throws IOException {
		// Constant.R : LINUX = '\n', WINDOWS = '\r\n'
		byte[] inputBytes = (Constant.R + lineStr).getBytes();
		ByteBuffer byteBuffer = ByteBuffer.wrap(inputBytes);
		try {
			progressFileChannels.write(byteBuffer);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void closeProgressFileChannels() throws Exception {
		if (progressFileChannels != null && progressFileChannels.isOpen()) {
			progressFileChannels.close();
		}
	}
	
	public void closeFileChannels(String table_nm) throws Exception {
		if (fileChannels != null && fileChannels.isOpen()) {
			try {
				WriterVO wv = new WriterVO();

				LogUtils.debug(String.format(msgCode.getCode("C0204"),table_nm,successByteCount),FileWriter.class);
				LogUtils.debug(String.format(msgCode.getCode("C0205"),table_nm,errByteCount),FileWriter.class);

				wv.setProcessLines(0);
				wv.setProcessBytes(successByteCount);
				wv.setPorcessErrorLines(errByteCount);

				fileChannels.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (badFileChannels != null && badFileChannels.isOpen()) {
			badFileChannels.close();
		}
	}
	
	public void closeBadFileChannels() throws Exception {
		if (badFileChannels != null && badFileChannels.isOpen()) {
			badFileChannels.close();
		}
	}
}
