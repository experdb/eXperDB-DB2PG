package com.k4m.experdb.db2pg.writer;

import java.io.IOException;

public interface Writer{

	/**
	 * Writer Call
	 * @param 
	 * @throws IOException 
	 * @throws Exception
	 */
	public boolean write(String wrt_nm, String table_nm, String lineStr) throws IOException;
	
}
