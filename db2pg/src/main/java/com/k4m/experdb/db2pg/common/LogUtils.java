package com.k4m.experdb.db2pg.common;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Marker;


public class LogUtils {
	private final static Map<Class<?>,org.slf4j.Logger> logs = new LinkedHashMap<Class<?>,org.slf4j.Logger>();
	private static boolean verbose = true;
	
	public static org.slf4j.Logger getLogger(Class<?> clazz) {
		org.slf4j.Logger logger = logs.get(clazz);
		if(logger == null) {
			logger = org.slf4j.LoggerFactory.getLogger(clazz);
			logs.put(clazz, logger);
		}
		return logger;
	}
	
	
	public static void conOnlyPrint(StringBuffer sb) {
		conOnlyPrint(sb.toString());
	}
	
	public static void conOnlyPrint(StringBuilder sb) {
		conOnlyPrint(sb.toString());
	}
	
	public static void conOnlyPrint(String paramString) {
		System.out.print(paramString);
	}
	public static void conOnlyPrintln(StringBuffer sb) {
		conOnlyPrintln(sb.toString());
	}
	
	public static void conOnlyPrintln(StringBuilder sb) {
		conOnlyPrintln(sb.toString());
	}
	
	public static void conOnlyPrintln(String paramString) {
		System.out.println(paramString);
	}


	//region trace
	public static void trace(String paramString,Class<?> clazz) {
		getLogger(clazz).trace(paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(String paramString,Class<?> clazz, Object paramObject) {
		getLogger(clazz).trace(paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(String paramString,Class<?> clazz, Object paramObject1, Object paramObject2) {
		getLogger(clazz).trace(paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(String paramString,Class<?> clazz, Object... paramArrayOfObject) {
		getLogger(clazz).trace(paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(String paramString,Class<?> clazz, Throwable paramThrowable) {
		getLogger(clazz).trace(paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(Marker paramMarker,Class<?> clazz, String paramString) {
		getLogger(clazz).trace(paramMarker, paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject) {
		getLogger(clazz).trace(paramMarker, paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject1, Object paramObject2) {
		getLogger(clazz).trace(paramMarker, paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(Marker paramMarker,Class<?> clazz, String paramString, Object... paramArrayOfObject) {
		getLogger(clazz).trace(paramMarker, paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void trace(Marker paramMarker,Class<?> clazz, String paramString, Throwable paramThrowable) {
		getLogger(clazz).trace(paramMarker, paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}
	//endregion

	//region debug
	public static void debug(String paramString,Class<?> clazz) {
		getLogger(clazz).debug(paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(String paramString,Class<?> clazz, Object paramObject) {
		getLogger(clazz).debug(paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(String paramString,Class<?> clazz, Object paramObject1, Object paramObject2) {
		getLogger(clazz).debug(paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(String paramString,Class<?> clazz, Object... paramArrayOfObject) {
		getLogger(clazz).debug(paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(String paramString,Class<?> clazz, Throwable paramThrowable) {
		getLogger(clazz).debug(paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(Marker paramMarker,Class<?> clazz, String paramString) {
		getLogger(clazz).debug(paramMarker, paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject) {
		getLogger(clazz).debug(paramMarker, paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject1, Object paramObject2) {
		getLogger(clazz).debug(paramMarker, paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(Marker paramMarker,Class<?> clazz, String paramString, Object... paramArrayOfObject) {
		getLogger(clazz).debug(paramMarker, paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void debug(Marker paramMarker,Class<?> clazz, String paramString, Throwable paramThrowable) {
		getLogger(clazz).debug(paramMarker, paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}
	//endregion
	
	//region info
	public static void info(String paramString,Class<?> clazz) {
		getLogger(clazz).info(paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void info(String paramString,Class<?> clazz, Object paramObject) {
		getLogger(clazz).info(paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void info(String paramString,Class<?> clazz, Object paramObject1, Object paramObject2) {
		getLogger(clazz).info(paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void info(String paramString,Class<?> clazz, Object... paramArrayOfObject) {
		getLogger(clazz).info(paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void info(String paramString,Class<?> clazz, Throwable paramThrowable) {
		getLogger(clazz).info(paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}

	public static void info(Marker paramMarker,Class<?> clazz, String paramString) {
		getLogger(clazz).info(paramMarker, paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void info(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject) {
		getLogger(clazz).info(paramMarker, paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void info(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject1, Object paramObject2) {
		getLogger(clazz).info(paramMarker, paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void info(Marker paramMarker,Class<?> clazz, String paramString, Object... paramArrayOfObject) {
		getLogger(clazz).info(paramMarker, paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void info(Marker paramMarker,Class<?> clazz, String paramString, Throwable paramThrowable) {
		getLogger(clazz).info(paramMarker, paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}
	//endregion
	
	//region warn
	public static void warn(String paramString,Class<?> clazz) {
		getLogger(clazz).warn(paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(String paramString,Class<?> clazz, Object paramObject) {
		getLogger(clazz).warn(paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(String paramString,Class<?> clazz, Object paramObject1, Object paramObject2) {
		getLogger(clazz).warn(paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(String paramString,Class<?> clazz, Object... paramArrayOfObject) {
		getLogger(clazz).warn(paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(String paramString,Class<?> clazz, Throwable paramThrowable) {
		getLogger(clazz).warn(paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(Marker paramMarker,Class<?> clazz, String paramString) {
		getLogger(clazz).warn(paramMarker, paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject) {
		getLogger(clazz).warn(paramMarker, paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject1, Object paramObject2) {
		getLogger(clazz).warn(paramMarker, paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(Marker paramMarker,Class<?> clazz, String paramString, Object... paramArrayOfObject) {
		getLogger(clazz).warn(paramMarker, paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void warn(Marker paramMarker,Class<?> clazz, String paramString, Throwable paramThrowable) {
		getLogger(clazz).warn(paramMarker, paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}
	//endregion
	
	//region error
	public static void error(String paramString,Class<?> clazz) {
		getLogger(clazz).error(paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void error(String paramString,Class<?> clazz, Object paramObject) {
		getLogger(clazz).error(paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void error(String paramString,Class<?> clazz, Object paramObject1, Object paramObject2) {
		getLogger(clazz).error(paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void error(String paramString,Class<?> clazz, Object... paramArrayOfObject) {
		getLogger(clazz).error(paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void error(String paramString,Class<?> clazz, Throwable paramThrowable) {
		getLogger(clazz).error(paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}

	public static void error(Marker paramMarker,Class<?> clazz, String paramString) {
		getLogger(clazz).error(paramMarker, paramString);
		if(verbose) System.out.println(paramString);
	}

	public static void error(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject) {
		getLogger(clazz).error(paramMarker, paramString, paramObject);
		if(verbose) System.out.println(paramString);
	}

	public static void error(Marker paramMarker,Class<?> clazz, String paramString, Object paramObject1, Object paramObject2) {
		getLogger(clazz).error(paramMarker, paramString, paramObject1, paramObject2);
		if(verbose) System.out.println(paramString);
	}

	public static void error(Marker paramMarker,Class<?> clazz, String paramString, Object... paramArrayOfObject) {
		getLogger(clazz).error(paramMarker, paramString, paramArrayOfObject);
		if(verbose) System.out.println(paramString);
	}

	public static void error(Marker paramMarker,Class<?> clazz, String paramString, Throwable paramThrowable) {
		getLogger(clazz).error(paramMarker, paramString, paramThrowable);
		if(verbose) System.out.println(paramString);
	}
	//endregion

	public static void setVerbose(boolean verbose) {
		LogUtils.verbose = verbose;
	}

}
