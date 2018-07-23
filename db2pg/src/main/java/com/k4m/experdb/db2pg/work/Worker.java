package com.k4m.experdb.db2pg.work;

public interface Worker {
	public void run();
	public void stop();
	public void shutdown();
	public boolean isRunning();
}
