package com.k4m.experdb.db2pg.db.oracle.spatial.geometry;

public class SDO_ELEM_INFO {
	int SDO_STARTING_OFFSET, SDO_ETYPE, SDO_INTERPRETATION;
	public SDO_ELEM_INFO(int SDO_STARTING_OFFSET, int SDO_ETYPE, int SDO_INTERPRETATION) {
		this.SDO_STARTING_OFFSET = SDO_STARTING_OFFSET;
		this.SDO_ETYPE = SDO_ETYPE;
		this.SDO_INTERPRETATION = SDO_INTERPRETATION;
	}
	@Override
	public String toString() {
		return "[OFFSET=" + SDO_STARTING_OFFSET
				+ ", ETYPE=" + SDO_ETYPE + ", INTERPRETATION="
				+ SDO_INTERPRETATION + "]";
	}
	
	
}
