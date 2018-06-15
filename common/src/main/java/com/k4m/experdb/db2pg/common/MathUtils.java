package com.k4m.experdb.db2pg.common;

public class MathUtils {
	public static long pow (long a, int b) {
	    if ( b == 0)	return 1;
	    if ( b == 1)	return a;
	    if (isEven( b )){	
	    	return pow ( a * a, b/2); //even a=(a^2)^b/2
	    } else {	
	    	return a * pow ( a * a, b/2); //odd  a=a*(a^2)^b/2
	    }
	}
	
	public static boolean isEven (long b) {
		if(b % 2 == 0) return true;
		return false;
	}
}
