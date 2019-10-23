package com.k4m.experdb.db2pg.convert.exception;

import com.k4m.experdb.db2pg.config.MsgCode;

public class NotSupportDatabaseTypeException extends Exception {
	static MsgCode msgCode = new MsgCode();
	private static final long serialVersionUID = -5891385551413682170L;
	
	public NotSupportDatabaseTypeException(String dbtype) {
		super(dbtype + msgCode.getCode("C0050"));
	}
	
}
