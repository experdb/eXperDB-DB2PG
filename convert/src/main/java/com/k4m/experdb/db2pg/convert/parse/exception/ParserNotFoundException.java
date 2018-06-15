package com.k4m.experdb.db2pg.convert.parse.exception;

public class ParserNotFoundException extends Exception {
	private static final long serialVersionUID = -2608995205774567716L;

	public ParserNotFoundException() {
		super("Parser Not Found");
	}
}
