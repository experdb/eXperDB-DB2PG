package com.k4m.experdb.db2pg.writer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

import com.k4m.experdb.db2pg.config.ConfigInfo;

public class FileWriter {
	protected String outputDirectory = ConfigInfo.OUTPUT_DIRECTORY + "data/";
	private  FileChannel fileChannels;
	private  FileChannel badFileChannels ;
	private static int successByteCount; // Writer가 처리한 총 Byte 수
	private static int errByteCount = 0; // 
	public static ConcurrentHashMap<String, ConfigInfo> ConnInfoList = new ConcurrentHashMap<String, ConfigInfo>();
	
	public FileWriter(){}
	
	public FileWriter(String table_nm) throws IOException{		
			fileCreater(outputDirectory + table_nm+ ".out");			
	}
	
	public boolean dataWriteToFile(String lineStr, String table_nm) throws IOException {
		byte[] inputBytes = (lineStr).getBytes();
		ByteBuffer byteBuffer = ByteBuffer.wrap(inputBytes);
		try {
			///if (lineStr.contains("시스템명")) {
			//	throw new Exception();
			//}
			successByteCount += fileChannels.write(byteBuffer);
		} catch (Exception e) {
			// bad파일 생성
			badFileCreater(outputDirectory + table_nm + ".bad");
			badFileWrite(lineStr);
			e.printStackTrace();
			return false;
		}
		return true;
	}
	

	public void badFileWrite(String lineStr) throws IOException {
		byte[] inputBytes = (lineStr + "\r\n").getBytes();
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
		if(!file.isFile()){
			fileChannels = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		}else{
			fileChannels = FileChannel.open(file.toPath(), StandardOpenOption.APPEND);
		}
	}

	public void badFileCreater(String file_nm) throws IOException {
		File file = new File(file_nm);
		if(!file.isFile()){
			badFileChannels = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		}else{
			badFileChannels = FileChannel.open(file.toPath(), StandardOpenOption.APPEND);
		}
	}
	
	
	public void closeFileChannels() throws Exception {
		if (fileChannels != null && fileChannels.isOpen()) {
			try {
				WriterVO wv = new WriterVO();

				System.out.println("Writer가 성공한 총 Byte 수 =" + successByteCount);
				System.out.println("Writer가 실패한 총 Byte 수 =" + errByteCount);
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
	
}
