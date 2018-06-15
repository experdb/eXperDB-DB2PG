package com.k4m.experdb.db2pg.db;

public class CallableParameter {
	private final CallableType type;
	private final Integer outtype;
	private Object value;	
	
	public CallableParameter(Object value){
		this.type = CallableType.IN;
		this.outtype = null;
		this.value = value;
	}
	
	public CallableParameter(int outtype){
		this.type = CallableType.OUT;
		this.outtype = outtype;
		this.value = null;
	}
	
	public CallableType getType(){
		return type;
	}
	
	public Object getValue(){
		return value;
	}
	
	public int getOutValueType(){
		return outtype;
	}
	
	public void setValue(Object value){
		this.value = value;
	}
}
