package com.k4m.experdb.db2pg.db.datastructure.exception;

public class DBTypeNotFoundException extends Exception {

	private static final long serialVersionUID = 7093618675906953081L;

	public DBTypeNotFoundException() {
		this("Not Exist DB Type");
	}

	public DBTypeNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public DBTypeNotFoundException(String message) {
		super(message);
	}

}
