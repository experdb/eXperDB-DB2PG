package com.k4m.experdb.db2pg.work;

public interface Worker extends Runnable {
	public void run();
	public void stop();
	public boolean isRunning();
	public boolean hasException();
	public Exception getException();
}
