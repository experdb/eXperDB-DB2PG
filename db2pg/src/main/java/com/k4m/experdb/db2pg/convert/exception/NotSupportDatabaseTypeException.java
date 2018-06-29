package com.k4m.experdb.db2pg.convert.exception;

public class NotSupportDatabaseTypeException extends Exception {
	private static final long serialVersionUID = -5891385551413682170L;
	
	public NotSupportDatabaseTypeException(String dbtype) {
		super(dbtype + " is not supported");
	}
	
}
