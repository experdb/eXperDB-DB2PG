package com.k4m.experdb.db2pg.work;

public abstract class DBWorker implements Worker {
	protected boolean isRunning = false; 
	protected boolean hasException = false;
	protected Exception exception = null;
	
	public DBWorker() {
	}
	
	@Override
	public boolean isRunning() {
		return isRunning;
	}

	public boolean hasException() {
		return hasException;
	}

	public Exception getException() {
		return exception;
	}
	
}
