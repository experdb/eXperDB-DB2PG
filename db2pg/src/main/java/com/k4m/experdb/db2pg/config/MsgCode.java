package com.k4m.experdb.db2pg.config;

import java.io.IOException;
import java.util.Properties;

import com.k4m.experdb.db2pg.sample.SampleFileLoader;

public class MsgCode implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	
	private static Properties Code;
	
	public void loadCode() throws IOException{
		Code = new Properties();
		Code.load(SampleFileLoader.getResourceInputStream("com/k4m/experdb/db2pg/config/MsgCode.config"));
	}


	public String getCode(String code) {
		return Code.getProperty(code);
	}
}
