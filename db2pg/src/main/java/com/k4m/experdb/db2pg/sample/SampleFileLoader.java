package com.k4m.experdb.db2pg.sample;

import java.io.InputStream;

public class SampleFileLoader {
	public static InputStream getResourceInputStream(String path) {
		ClassLoader classLoader = SampleFileLoader.class.getClassLoader();
		return classLoader.getResourceAsStream(path);
	}
}
