package com.k4m.experdb.db2pg.convert.vo;

import java.util.regex.Pattern;

public class ConvertVO {
	private String asValue;
	private String toValue;
	private Pattern pattern;
	
	public ConvertVO(String asValue, String toValue) {
		this.asValue = asValue;
		this.toValue = toValue;
		pattern = null;
		if(asValue.startsWith("^")) {
			if(asValue.lastIndexOf("$") != -1) {
				pattern = Pattern.compile(asValue);
			}
		}
	}
	
	public String getAsValue() {
		return asValue;
	}
	public String getToValue() {
		return toValue;
	}
	public Pattern getPattern() {
		return pattern;
	}
//	
//	public void setAsValue(String asValue) {
//		this.asValue = asValue;
//	}
//	public void setToValue(String toValue) {
//		this.toValue = toValue;
//	}
//	public void setPattern(Pattern pattern) {
//		this.pattern = pattern;
//	}

	@Override
	public String toString() {
		return "[" + asValue + ", " + toValue + "]";
	}
	
	
}
