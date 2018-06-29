package com.k4m.experdb.db2pg.db.oracle.spatial.geometry;

public interface SDO_GTYPE {
	public final static int POINT=1, LINESTRING=2, POLYGON_OR_SURFACE=3, GEOMETRYCOLLECTION=4
			, MULTIPOINT=5, MULTILINESTRING=6, MULTIPOLYGON_OR_MULTISURFACE=7
			/**
			 * 수정자 : 이재원
			 * 날짜 : 2017-07-03
			 * SURFACE, SOLID 추가 */
			, SOLID=8, MULTISOLID=9
			;
}
