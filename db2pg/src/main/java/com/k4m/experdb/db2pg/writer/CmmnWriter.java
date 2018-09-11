package com.k4m.experdb.db2pg.writer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.k4m.experdb.db2pg.config.ConfigInfo;

public class CmmnWriter implements Writer{

	private String wrt_nm;
	private String table_nm;
	private String lineStr;

	public CmmnWriter(){}

	public CmmnWriter(String wrt_nm, String table_nm, String lineStr) throws IOException {
		this.wrt_nm = wrt_nm;
		this.table_nm = table_nm;
		this.lineStr = lineStr;
	}

	
	public static void main(String[] args) throws Exception {
		Writer wt = new CmmnWriter();
		BufferedReader br = new BufferedReader(new FileReader("C:\\test\\read.txt"));
		String lineStr = null;
		while ((lineStr = br.readLine()) != null) {
			wt.write("db", lineStr, "db_test");
		}
		br.close();

		/*System.out.println("DB Writer 된 총 로우 수 : "+wv.getProcessLines());
		System.out.println("처리된 총 bytes 수 : "+wv.getProcessBytes());
		System.out.println("적재가 실패한 총 로우 수 : "+wv.getPorcessErrorLines());*/
	}
	

	@Override
	public boolean write(String wrt_nm, String lineStr, String table_nm) throws IOException {
		ConfigInfo.OUTPUT_DIRECTORY = "C:\\test\\";
		FileWriter fw = new FileWriter(lineStr, table_nm);
		DBWriter dw = new DBWriter(); 
		// FileWriter
		if (wrt_nm == "file") {
			fw.FileWrite(lineStr, table_nm);
			// DBWriter
		} else {
			dw.DBWrite(lineStr, table_nm);
		}
		return true;
	}

}
