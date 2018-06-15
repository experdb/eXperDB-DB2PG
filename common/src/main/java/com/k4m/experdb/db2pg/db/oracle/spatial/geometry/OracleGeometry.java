package com.k4m.experdb.db2pg.db.oracle.spatial.geometry;

import java.util.Arrays;
import java.util.List;

public class OracleGeometry {
	int dim, lrs, gtype,srid;
	String suffix;
	double[] point, ordinates;
	List<SDO_ELEM_INFO> sdoElemInfoList;
	List<double[]> coords;
	public OracleGeometry(){
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("");
		sb.append("OracleGeometry [dim=" + dim + ", lrs=" + lrs + ", gtype="
				+ gtype + ", srid=" + srid + ", suffix=" + suffix + ", point="
				+ Arrays.toString(point) + ", ordinates="
				+ Arrays.toString(ordinates) + ", sdoElemInfoList="
				+ sdoElemInfoList + ", coords=[" );
		for(int i=0;i<coords.size();i++){
			sb.append(Arrays.toString(coords.get(i)));
			if(i<coords.size()-1)
				sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}
	
	
}
