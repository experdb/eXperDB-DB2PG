package com.k4m.experdb.db2pg.db.oracle.spatial.geometry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.spatial.geometry.JGeometry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Process {
	private static final Logger logger = LoggerFactory.getLogger(Process.class);

	//region parse
		public static String parseSdoGeometry(Connection oraConn, JGeometry jGeometry) {
			if(jGeometry == null) return "";
			
			OracleGeometry oGeometry = new OracleGeometry();
			oGeometry.gtype = jGeometry.getType();
			oGeometry.dim = jGeometry.getDimensions();
			oGeometry.lrs = jGeometry.getLRMDimension();
			if(oGeometry.dim<2) {
				logger.error("ERROR: Dimension "+oGeometry.dim+" is not valid. Either specify a dimension or use Oracle Locator Version 9i or later.");
				return "";
			}
			
			
			if(oGeometry.dim ==3) {
				oGeometry.suffix="Z";
			} else if(oGeometry.dim ==4) {
				oGeometry.suffix="ZM";
			} else {
				oGeometry.suffix="";
			}
//			try {
				oGeometry.srid = 4326;
//				oGeometry.srid = pridOraToPg(oraConn,jGeometry.getSRID());
//			} catch(SQLException sqle) {
//				logger.error("ERROR: Fail get srid");
//				return "";
//			}
			oGeometry.point = jGeometry.getPoint();
			oGeometry.sdoElemInfoList = parseSdoElemInfo(jGeometry.getElemInfo());
			oGeometry.ordinates = jGeometry.getOrdinatesArray();
			String value = extractGeometry(oraConn,oGeometry);
			if(value != null) return String.format("SRID=%d;%s",oGeometry.srid,value);
			return "";
		}
		//endregion
		
		//region extract
		public static String extractGeometry(Connection oraConn, OracleGeometry oGeometry) {
			if ( oGeometry.gtype == 1 && oGeometry.point != null && oGeometry.sdoElemInfoList == null){
				if(oGeometry.sdoElemInfoList == null) oGeometry.sdoElemInfoList = new ArrayList<SDO_ELEM_INFO>();
				if(oGeometry.coords==null) oGeometry.coords = new ArrayList<double[]>();
				oGeometry.coords.add(oGeometry.point);
				oGeometry.sdoElemInfoList.add(new SDO_ELEM_INFO(1, SDO_ETYPE.POINT, INTERPRETATION_POINT.SIMPLE_POINT));
			} else {
				coordinates(oGeometry);
			}
			
			if(oGeometry.gtype == SDO_GTYPE.POINT) {
				return createPoint(oGeometry,0);
			} 
			if(oGeometry.gtype == SDO_GTYPE.LINESTRING) {
				if(oGeometry.sdoElemInfoList.get(0).SDO_ETYPE == SDO_ETYPE.COMPOUNDCURVE) {
					return createCompoundLine(oGeometry,1,-1);
				} else {
					return createLine(oGeometry,0);
				}
			} 
			if (oGeometry.gtype == SDO_GTYPE.POLYGON_OR_SURFACE) {
				
				if(oGeometry.sdoElemInfoList.get(0).SDO_ETYPE == SDO_ETYPE.SURFACE_EXTERIOR) {
					return createSurface(oGeometry, 0);
				} else {			
					return createPolygon(oGeometry,0);
				}
			}
			if (oGeometry.gtype == SDO_GTYPE.MULTIPOINT) {
				return createMultiPoint(oGeometry,0);
			}
			if (oGeometry.gtype == SDO_GTYPE.MULTILINESTRING) {
				return createMultiLine(oGeometry,0,-1);
			}
			if (oGeometry.gtype == SDO_GTYPE.MULTIPOLYGON_OR_MULTISURFACE) {
				return createMultiPolygon(oGeometry,0,-1);
			}
			if (oGeometry.gtype == SDO_GTYPE.GEOMETRYCOLLECTION) {
				return createCollection(oGeometry,0,-1);
			}
			if (oGeometry.gtype == SDO_GTYPE.SOLID) {
				return createSolid(oGeometry,0);
			}
			if (oGeometry.gtype == SDO_GTYPE.MULTISOLID) {
//				for(SDO_ELEM_INFO elem : oGeometry.sdoElemInfoList) {
//					System.out.println(elem);
//				}
//				System.out.println();
				return createMultiSolid(oGeometry,0,-1);
			}
			
			return null;
		}
		//endregion
		
		//region creates 
		//region Point
		public static String createPoint(OracleGeometry oGeometry, int elemIndex) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			if(sOffset<1 || sOffset > oGeometry.coords.size()*oGeometry.dim) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+oGeometry.coords.size()*oGeometry.dim+"Offset inconsistent with ordinates length ");
			}
			if(etype!=SDO_ETYPE.POINT){
				logger.error("ERROR: SDO_ETYPE "+etype+" inconsistent with expected POINT");
			}
			
			if(interpretation > 1) {
				return createMultiPoint(oGeometry,elemIndex);
			} else if(interpretation == 0) {
				logger.error("ERROR: SDO_ETYPE.POINT requires interpretation >= 1");
				return null;
			}
			
			int start = (sOffset -1) / oGeometry.dim;
			int eOffset = getStartOffset(oGeometry, elemIndex+1);
//			int end = eOffset > -1?(eOffset-1) / oGeometry.dim:oGeometry.coords.size();
			int end = eOffset != -1?(eOffset-1)/oGeometry.dim:oGeometry.coords.size();
			String point = setCoordicates(oGeometry.coords,start+1,end);
			return String.format("POINT%s (%s)", oGeometry.suffix,point);
		}
		//endregion
		
		//region MultiPoint
		public static String createMultiPoint(OracleGeometry oGeometry, int elemIndex) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			while(oGeometry.sdoElemInfoList.get(elemIndex).SDO_ETYPE == 0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry, elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			int length = oGeometry.coords.size() * oGeometry.dim;
			if((sOffset <1) || (sOffset>length)) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length "+oGeometry.coords.size());
			}
			if(oGeometry.sdoElemInfoList.get(elemIndex).SDO_ETYPE == SDO_ETYPE.POINT){
				logger.error("ERROR: SDO_ETYPE "+SDO_ETYPE.POINT+" inconsistent with expected POINT");
			}
			List<String> points = new ArrayList<String>();
			int start = (sOffset-1) / oGeometry.dim;
			
			if(interpretation > 1) {
				for(int i=start+1;i<=start+interpretation;i++){
					String coordicates = setCoordicates(oGeometry.coords, i, i);
					points.add(coordicates);
				}
			} else if (interpretation != 0) {
				int cont =1;
				for (int i = start + 1; cont != 0 && (etype=eType(oGeometry,i-1)) != -1; i++) {
					if (etype == 0) continue;
					if (interpretation(oGeometry,i - 1) == 0)continue;
					if (etype == SDO_ETYPE.POINT) {
						points.add(setCoordicates(oGeometry.coords, i, i));
					} else {
						cont = 0;
					}
				}
			}
			StringBuffer sb = new StringBuffer("MULTIPOINT");
			sb.append(oGeometry.suffix);
			sb.append(" ((");
			for(int i=0;i<points.size();i++){
				sb.append(points.get(i));
				if(i<points.size()-1)
					sb.append("), (");
			}
			sb.append("))");
			return sb.toString();
		}
		//endregion
		
		//region Line
		public static String createLine(OracleGeometry oGeometry, int elemIndex) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			if(etype != SDO_ETYPE.LINESTRING) {
				return null;
			}
			
			int start = (sOffset -1) / oGeometry.dim;
			int eOffset = getStartOffset(oGeometry, elemIndex+1);
			int end = eOffset != -1?(eOffset-1)/oGeometry.dim:oGeometry.coords.size();
			if(oGeometry.sdoElemInfoList.get(0).SDO_ETYPE == SDO_ETYPE.COMPOUNDCURVE) {
				end++;
			}
			if(interpretation != 1) {
				return String.format("CIRCULARSTRING%s (%s)", oGeometry.suffix,setCoordicates(oGeometry.coords, start+1, end));
			}
			return String.format("LINESTRING%s (%s)", oGeometry.suffix,setCoordicates(oGeometry.coords, start+1, end));
		}
		//endregion
		
		//region MultiLine
		public static String createMultiLine(OracleGeometry oGeometry, int elemIndex, int numGeom) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			int length = oGeometry.coords.size() * oGeometry.dim;
			
			if(sOffset <1 || sOffset > length) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length "+oGeometry.coords.size());
			}
			if(etype != SDO_ETYPE.LINESTRING) {
				logger.error("ERROR: SDO_ETYPE "+etype+" inconsistent with expected LINESTRING");
			}
			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : oGeometry.sdoElemInfoList.size();
			List<String> lines = new ArrayList<String>();
			int cont=1;
			for(int i=elemIndex; cont!=0 && i  < endTriplet && (etype = eType(oGeometry,i)) != -1; i++) {
				if (etype==0) continue;
				
				if(etype == SDO_ETYPE.LINESTRING) {
					lines.add(createLine(oGeometry, i));
				} else {
					cont = 0;
				}
			}
			
			if(interpretation > 1) {
				return String.format("MULTICURVE%s (%s)", oGeometry.suffix,join(", ",lines));
			}
			
			for(int i=0;i<lines.size();i++){
				String tmp = removeStr(lines.get(i),String.format("LINESTRING%s ", oGeometry.suffix));
				if(tmp!=null) {
					lines.remove(i);
					lines.add(i,tmp);
				}
			}
			return String.format("MULTILINESTRING%s (%s)",oGeometry.suffix, join(", ",lines));
		}
		//endregion
		
		//region CompoundLine
		public static String createCompoundLine(OracleGeometry oGeometry, int elemIndex, int numGeom) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			int length = oGeometry.coords.size() * oGeometry.dim;
			
			if(sOffset < 1 || sOffset > length) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length "+oGeometry.coords.size());
			}
			if(etype != SDO_ETYPE.LINESTRING) {
				logger.error("ERROR: SDO_ETYPE "+etype+" inconsistent with expected LINESTRING");
			}
			
//			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : ((oGeometry.sdoElemInfoList.size()+1) / 3);
			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : oGeometry.sdoElemInfoList.size();
			List<String> lines = new ArrayList<String>();
			int cont=1;
			for(int i=elemIndex; cont != 0 && i  < endTriplet && (etype = eType(oGeometry,i)) != -1; i++) {
				if (etype==0) continue;
				
				if(etype == SDO_ETYPE.LINESTRING) {
					String line = createLine(oGeometry, i);
					lines.add(line);
				} else {
					cont = 0;
				}
			}
			
			StringBuffer sb = new StringBuffer("COMPOUNDCURVE");
			sb.append(oGeometry.suffix);
			sb.append(" (");
			for(int i=0;i<lines.size();i++){
				sb.append(lines.get(i));
				if(i<lines.size()-1)
					sb.append(", ");
			}
			sb.append(")");
			
			return sb.toString();
		}
		//endregion
		
		//region LinearRing
		public static String createLinearRing(OracleGeometry oGeometry, int elemIndex) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			int length = oGeometry.coords.size() * oGeometry.dim;
			
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			//Exclude type 0 (zero) element
			if(etype==0) return null;
			
			if(sOffset <1 || sOffset > length) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length "+oGeometry.coords.size());
			}
			if(etype == SDO_ETYPE.COMPOUND_POLYGON_INTERIOR || etype == SDO_ETYPE.COMPOUND_POLYGON_EXTERIOR) {
				return null;
			}
			
			StringBuffer ring = new StringBuffer("");
//			int start = 0;
			int start = (sOffset-1) / oGeometry.dim;
//			if(sOffset>=oGeometry.dim) start=(sOffset-1)/oGeometry.dim;
			if((etype == SDO_ETYPE.POLYGON_EXTERIOR || etype == SDO_ETYPE.POLYGON_INTERIOR) && interpretation == 3) {
				if (oGeometry.dim == 3) {
					return make3dRectangle(oGeometry,start).replace("((", "(").replace("))", ")");
				} else {
					double[] min = oGeometry.coords.get(start);
					double[] max = oGeometry.coords.get(start+1);
					ring.append(join(" ",min));
					ring.append(", "); 
					ring.append(min[0]); ring.append(" "); ring.append(max[1]);
//					ring.append(max[0]); ring.append(" "); ring.append(min[1]); 
					ring.append(", ");
					ring.append(join(" ",max));
					ring.append(", "); 
					ring.append(max[0]); ring.append(" "); ring.append(min[1]); 
//					ring.append(min[0]); ring.append(" "); ring.append(max[1]);
					ring.append(", ");
					ring.append(join(" ",min));
				}
			} else {
				int eOffset = getStartOffset(oGeometry, elemIndex+1);
				int end = eOffset > -1?(eOffset-1) / oGeometry.dim:oGeometry.coords.size();
				
				if (etype != SDO_ETYPE.POLYGON || interpretation != 1) {
					//end++;
				}
				if(interpretation ==2) {
					if(etype==SDO_ETYPE.LINESTRING || etype == SDO_ETYPE.POLYGON_EXTERIOR || etype == SDO_ETYPE.POLYGON_INTERIOR) {
						end++;
					}
				}
				if(oGeometry.sdoElemInfoList.get(0).SDO_ETYPE == SDO_ETYPE.COMPOUND_POLYGON_INTERIOR || oGeometry.sdoElemInfoList.get(0).SDO_ETYPE == SDO_ETYPE.COMPOUND_POLYGON_EXTERIOR){
					end++;
				}
				String coordicates = setCoordicates(oGeometry.coords, start+1, end);
				ring.append(coordicates);
				if( interpretation == 4 ){
					ring.append(", ");
					ring.append(setCoordicates(oGeometry.coords, start+1, start+1));
				}
			}
			
			if(etype==SDO_ETYPE.POLYGON_EXTERIOR && interpretation ==2) {
				return String.format("CIRCULARSTRING%s (%s)", oGeometry.suffix,ring);
			} else if(etype==SDO_ETYPE.COMPOUND_POLYGON_EXTERIOR && interpretation ==2) {
				return String.format("COMPOUNDCURVE%s (%s)", oGeometry.suffix,ring);
			} else if(etype==SDO_ETYPE.LINESTRING) {
				return String.format("CIRCULARSTRING%s (%s)", oGeometry.suffix,ring);
			}
			return String.format("(%s)",ring);
		}
		//endregion
		
		//region Polygon	
		public static String createPolygon(OracleGeometry oGeometry, int elemIndex) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			if(sOffset<1 || sOffset > oGeometry.coords.size()*oGeometry.dim) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+"Offset inconsistent with COORDINATES length "+(oGeometry.coords.size()*oGeometry.dim));
			}
			
			List<String> rings = new ArrayList<String>();
			String ring = createLinearRing(oGeometry,elemIndex);
			if(ring!=null) rings.add(ring);
			int cont = 1;
			
			for (int i=elemIndex+1;((cont != 0) && ((etype =eType(oGeometry,i)))!= -1); i++ ) {
				if(etype ==0)continue;
				if(etype==SDO_ETYPE.LINESTRING) {
					ring = createLinearRing(oGeometry,i);
					if(ring!=null) rings.add(ring);
				} else if(etype == SDO_ETYPE.POLYGON_INTERIOR) {
					ring = createLinearRing(oGeometry,i);
					if(ring!=null) rings.add(ring);
				} else if(etype == SDO_ETYPE.COMPOUND_POLYGON_EXTERIOR) {
					continue;
				} else if(etype == SDO_ETYPE.POLYGON) {
					ring = createLinearRing(oGeometry,i);
					if(ring!=null) rings.add(ring);
				} else {
					cont = 0;
				}
			}
			
			StringBuffer poly = new StringBuffer("");
			if(interpretation ==2 || interpretation == 4) {
				if(oGeometry.sdoElemInfoList.get(0).SDO_ETYPE == SDO_ETYPE.COMPOUND_POLYGON_EXTERIOR) {
					String tmp = removeStr(rings.get(0),String.format("CIRCULARSTRING%s ", oGeometry.suffix));
					if(tmp!=null) {
						rings.remove(0);
						rings.add(0,tmp);
					}
					poly.append(String.format("CURVEPOLYGON%s (COMPOUNDCURVE%s (", oGeometry.suffix, oGeometry.suffix));
					for(int i=0;i<rings.size();i++){
						poly.append(rings.get(i));
						if(i<rings.size()-1){
							poly.append(", ");
						}
					}
					poly.append("))");
				} else {
					poly.append(String.format("CURVEPOLYGON%s (", oGeometry.suffix));
					for(int i=0;i<rings.size();i++){
						poly.append(rings.get(i));
						if(i<rings.size()-1){
							poly.append(", ");
						}
					}
					poly.append(")");
				}
			} else {
				poly.append(String.format("POLYGON%s (", oGeometry.suffix));
				for(int i=0;i<rings.size();i++){
					poly.append(rings.get(i));
					if(i<rings.size()-1){
						poly.append(", ");
					}
				}
				poly.append(")");
			}
			return poly.toString();
		}
		//endregion
		
		//region MultiPolygon
		public static String createMultiPolygon(OracleGeometry oGeometry, int elemIndex, int numGeom) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			int length = oGeometry.coords.size() * oGeometry.dim;
			
			if(sOffset <1 || sOffset > length) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length "+oGeometry.coords.size());
			}
			if(etype != SDO_ETYPE.POLYGON && etype != SDO_ETYPE.POLYGON_EXTERIOR) {
				logger.error("ERROR: SDO_ETYPE "+etype+" inconsistent with expected POLYGON or POLYGON_EXTERIOR");
			}
			if(interpretation != 1 && interpretation != 3) {
				return null;
			}
			
//			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : ((oGeometry.sdoElemInfoList.size()*3+1) / 3) + 1;
			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : oGeometry.sdoElemInfoList.size();
			
			
			List<String> polys = new ArrayList<String>();
			int cont=1;
			
			for(int i=elemIndex; cont !=0 && i  < endTriplet && (etype = eType(oGeometry,i)) != -1; i++) {
				if (etype==0) continue;
				
				if(etype == SDO_ETYPE.POLYGON || etype == SDO_ETYPE.POLYGON_EXTERIOR ) {
					String poly = removeStr(createPolygon(oGeometry,i),String.format("POLYGON%s ", oGeometry.suffix));
					if(etype != eType(oGeometry, i-1)) {
						if(etype == SDO_ETYPE.POLYGON_INTERIOR && SDO_ETYPE.POLYGON_EXTERIOR == eType(oGeometry,i-1)) {
							poly = poly.substring(poly.indexOf("(")+1);
							String tmp = polys.get(polys.size()-1);
							tmp = tmp.substring(0,tmp.lastIndexOf(")"));
							polys.remove(polys.size()-1);
							polys.add(polys.size()-1,tmp);
						}
					}
					polys.add(poly);
					while(eType(oGeometry, i+1) == SDO_ETYPE.POLYGON_INTERIOR) {
						i++;
					}
				} else {
					cont = 0;
				}
			}
			
			return String.format("MULTIPOLYGON%s (%s)", oGeometry.suffix,join(", ",polys));
		}
		//endregion
		
		//region Collection
		public static String createCollection(OracleGeometry oGeometry, int elemIndex, int numGeom) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int length = oGeometry.coords.size() * oGeometry.dim;
			
			if(sOffset > length)
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length " + oGeometry.coords.size());
			
//			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : (oGeometry.sdoElemInfoList.size() / 3) + 1;
//			int endTriplet = (oGeometry.sdoElemInfoList.size()+1)/3+1;
			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : oGeometry.sdoElemInfoList.size();
			List<String> geoms = new ArrayList<String>();
			int etype;
			int interpretation;
			String geom = null;
			
			int cont=1;
			for(int i=elemIndex; cont!=0 && i < endTriplet; i++){
				etype = eType(oGeometry, i);
				interpretation = interpretation(oGeometry, i);
				
				if(etype==0) continue;
				
				if(etype == -1) {
					cont = 0;
					continue;
				} else if ( etype == SDO_ETYPE.POINT ) {
					if(interpretation == 1) {
						geom = createPoint(oGeometry, i);
					} else if (interpretation > 1) {
						geom = createMultiPoint(oGeometry, i);
					}
				} else if ( etype == SDO_ETYPE.LINESTRING ) {
					geom = createLine(oGeometry, i);
				} else if ( etype == SDO_ETYPE.POLYGON || etype == SDO_ETYPE.POLYGON_EXTERIOR ) {
					geom = createPolygon(oGeometry, i);
					//Skip interior rings
					while(eType(oGeometry, i+1) == SDO_ETYPE.POLYGON_INTERIOR) i++;
				} else if ( etype == SDO_ETYPE.POLYGON_INTERIOR ) {
					logger.error(String.format("%s %s", "ERROR: SDO_ETYPE 2003 (Polygon Interior) no expected in a GeometryCollection"
							,"(2003 is used to represent polygon holes, in a 1003 polygon exterior)"));
					continue;
				} else {
					logger.error("ERROR: SDO_ETYPE "+etype+" not representable as a EWKT Geometry by DX-DB2PG");
					continue;
				}
				geoms.add(geom);
			}
			
			return String.format("GEOMETRYCOLLECTION%s (%s)", oGeometry.suffix,join(", ",geoms));
		}
		//endregion
		
		//region Surface
		public static String createSurface(OracleGeometry oGeometry, int elemIndex) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			
			int length = oGeometry.coords.size() * oGeometry.dim;
			
			if(sOffset <1 || sOffset > length) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length "+oGeometry.coords.size());
			}
//			if(etype != SDO_ETYPE.SURFACE_EXTERIOR) {
//				logger.error("ERROR: SDO_ETYPE "+etype+" inconsistent with expected SURFACE_EXTERIOR");
//			}
			
			boolean isExterior = etype == SDO_ETYPE.SURFACE_EXTERIOR;
			
//			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : oGeometry.sdoElemInfoList.size();
			int endTriplet = elemIndex + interpretation+1;
			
			List<String> surface = new ArrayList<String>();
			int cont=1;
			
			for(int i=elemIndex; cont !=0 && i<endTriplet && (etype = eType(oGeometry,i)) != -1; i++) {
				if (etype==0) continue;
				if(etype == SDO_ETYPE.POLYGON || etype == SDO_ETYPE.POLYGON_EXTERIOR ) {
					String poly = removeStr(createPolygon(oGeometry,i),String.format("POLYGON%s ", oGeometry.suffix));
					if(etype != eType(oGeometry, i-1)) {
						if(etype == SDO_ETYPE.POLYGON_INTERIOR && SDO_ETYPE.POLYGON_EXTERIOR == eType(oGeometry,i-1)) {
							poly = poly.substring(poly.indexOf("(")+1);
							
							String tmp = surface.get(surface.size()-1);
							tmp = tmp.substring(0,tmp.lastIndexOf(")"));
							surface.remove(surface.size()-1);
							surface.add(surface.size()-1,tmp);
						}
					}
					surface.add(poly);
					while(eType(oGeometry, i+1) == SDO_ETYPE.POLYGON_INTERIOR) {
						endTriplet++;
						i++;
					}
				} else if( etype == SDO_ETYPE.POLYGON_INTERIOR && !isExterior ) {
					String poly = removeStr(createPolygon(oGeometry,i),String.format("POLYGON%s ", oGeometry.suffix));
					surface.add(poly);
					while(eType(oGeometry, i) == SDO_ETYPE.POLYGON_INTERIOR) {
						i++;
					}
				} else if(etype == SDO_ETYPE.SURFACE_EXTERIOR ) {
					continue;
				} else if(etype == SDO_ETYPE.SURFACE_INTERIOR ) {
					continue;
				} else {
					cont = 0;
				}
			}
			
			return String.format("POLYHEDRALSURFACE%s (%s)", oGeometry.suffix,join(", ",surface));
		}
		//endregion
		
		//region Solid
		public static String createSolid(OracleGeometry oGeometry, int elemIndex) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			// Interpretation column must be 1 or 3
			if(etype == SDO_ETYPE.SOLID_EXTERIOR && interpretation != 1 && interpretation != 3) {
				return null;
			}
			// 3이면 box형
			
			if(etype == SDO_ETYPE.COMPOSITE_SOLID) {
				return createMultiSolid(oGeometry, elemIndex+1, -1);
			}
			
			if(sOffset < 1 || sOffset > oGeometry.coords.size()*oGeometry.dim) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+"Offset inconsistent with COORDINATES length "+(oGeometry.coords.size()*oGeometry.dim));
			}
			
			List<String> surfaces = new ArrayList<String>();
			if(etype == SDO_ETYPE.SOLID_EXTERIOR) {
				if(interpretation == 1) {
					elemIndex++;
					sOffset = getStartOffset(oGeometry, elemIndex);
					etype = eType(oGeometry,elemIndex);
					interpretation = interpretation(oGeometry,elemIndex);
					String tmp = createSurface(oGeometry, elemIndex);
					
					surfaces.add(removeStr(tmp.replace("((", "(").replace("))",")"), String.format("POLYHEDRALSURFACE%s ", oGeometry.suffix)));
					if((etype = eType(oGeometry,elemIndex+interpretation+1)) != -1){
						elemIndex += interpretation;
						if(etype == SDO_ETYPE.SURFACE_INTERIOR) {
							tmp = createSurface(oGeometry,elemIndex+1);
							surfaces.add(removeStr(tmp.replace("((", "(").replace("))",")"), String.format("POLYHEDRALSURFACE%s ", oGeometry.suffix)));
						}
					}
				} else if(interpretation == 3) {
					int start = (sOffset -1) / oGeometry.dim;
					return String.format("POLYHEDRALSURFACE%s (%s)", oGeometry.suffix,make3dRectangle(oGeometry,start));
				}
			}
			
			return String.format("POLYHEDRALSURFACE%s (%s)", oGeometry.suffix, join(", ",surfaces));
		}
		//endregion
		
		//region MultiSolid
		public static String createMultiSolid(OracleGeometry oGeometry, int elemIndex, int numGeom) {
			int sOffset = getStartOffset(oGeometry, elemIndex);
			int etype = eType(oGeometry, elemIndex);
			int interpretation = interpretation(oGeometry,elemIndex);
			
			while(etype==0) {
				elemIndex++;
				sOffset = getStartOffset(oGeometry, elemIndex);
				etype = eType(oGeometry,elemIndex);
				interpretation = interpretation(oGeometry,elemIndex);
			}
			
			int length = oGeometry.coords.size() * oGeometry.dim;
			
			if(sOffset <1 || sOffset > length) {
				logger.error("ERROR: SDO_ELEM_INFO starting offset "+sOffset+" inconsistent with ordinates length "+oGeometry.coords.size());
			}
			if(etype != SDO_ETYPE.SOLID_EXTERIOR ) {
				logger.error("ERROR: SDO_ETYPE "+etype+" inconsistent with expected SOLID_EXTERIOR ");
			}
			if(etype == SDO_ETYPE.SOLID_EXTERIOR && interpretation != 1 && interpretation != 3) {
				return null;
			}
			int endTriplet = (numGeom != -1) ? (elemIndex + numGeom) : oGeometry.sdoElemInfoList.size();
			
			List<String> solids = new ArrayList<String>();
			int cont=1;
			
			for(int i=elemIndex; cont !=0 && i  < endTriplet && (etype = eType(oGeometry,i)) != -1; i++) {
				if (etype==0) continue;
				if( etype == SDO_ETYPE.SOLID_EXTERIOR ) {
					String solid = removeStr(createSolid(oGeometry,i),String.format("POLYHEDRALSURFACE%s ", oGeometry.suffix));
					interpretation = interpretation(oGeometry,i);
					solid = solid.replace("((", "(").replace("))", ")");
					solids.add(solid);
					etype = eType(oGeometry, i+1);
					if(etype == SDO_ETYPE.SURFACE_EXTERIOR || etype == SDO_ETYPE.SURFACE_INTERIOR ) {
						i += interpretation(oGeometry,i+1)+1;
					}
				} else {
					cont = 0;
				}
				
			}
			return String.format("MULTISURFACE%s (%s)", oGeometry.suffix,join(", ",solids));
		}
		//endregion
		//endregion
		
		//region functions
		
		public static String make3dRectangle(OracleGeometry oGeometry, int start) {
			class TemporaryClazz {
				double[] min, max;
				int sameCount; 
				boolean xSame,ySame,zSame;
				
				TemporaryClazz() {
					sameCount = 0;
					xSame = false;
					ySame = false;
					zSame = false;
				}
				TemporaryClazz process(OracleGeometry oGeometry,int start) {
					min = new double[oGeometry.dim];
					max = new double[oGeometry.dim];
					double[] tmp1 = oGeometry.coords.get(start);
					double[] tmp2 = oGeometry.coords.get(start+1);
					
					for(int tmpIdx=0;tmpIdx<oGeometry.dim;tmpIdx++) {
						if(tmp1[tmpIdx]<tmp2[tmpIdx]) {
							min[tmpIdx] = tmp1[tmpIdx];
							max[tmpIdx] = tmp2[tmpIdx];
						} else if (tmp1[tmpIdx]>tmp2[tmpIdx]) {
							max[tmpIdx] = tmp1[tmpIdx];
							min[tmpIdx] = tmp2[tmpIdx];
						} else {
							min[tmpIdx] = tmp1[tmpIdx];
							max[tmpIdx] = tmp2[tmpIdx];
							sameCount++;
							if(tmpIdx==0) {
								xSame = true;
							} else if(tmpIdx==1) {
								ySame = true;
							} else if(tmpIdx==2) {
								zSame = true;
							}
						}
					}
					return this;
				}
			}
			TemporaryClazz tmp = new TemporaryClazz().process(oGeometry, start);
			
			double[][][] rects = {
				{{tmp.min[0],tmp.min[1],tmp.min[2]},{tmp.max[0],tmp.max[1],tmp.min[2]}}
				,{{tmp.min[0],tmp.min[1],tmp.max[2]},{tmp.max[0],tmp.max[1],tmp.max[2]}}
				,{{tmp.min[0],tmp.min[1],tmp.min[2]},{tmp.min[0],tmp.max[1],tmp.max[2]}}
				,{{tmp.max[0],tmp.min[1],tmp.min[2]},{tmp.max[0],tmp.max[1],tmp.max[2]}}
				,{{tmp.min[0],tmp.min[1],tmp.min[2]},{tmp.max[0],tmp.max[1],tmp.max[2]}}
				,{{tmp.min[0],tmp.max[1],tmp.min[2]},{tmp.max[0],tmp.max[1],tmp.max[2]}}
			}; 
			
			if(tmp.sameCount == 0) {
				List<String> rectStrs = new ArrayList<String>();
				for(int i=0 ;i< rects.length;i++) {
					double[][] rect = rects[i];
					double[] llb = rect[0], urt = rect[1];
					String rectStr = null;
					if(i==0) {
						rectStr = String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
								,llb[0],llb[1],llb[2],llb[0],urt[1],llb[2],urt[0],urt[1],llb[2],urt[0],llb[1],llb[2],llb[0],llb[1],llb[2]);
					} else if(i==1) {
						rectStr = String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
								,llb[0],llb[1],urt[2],llb[0],urt[1],urt[2],urt[0],urt[1],urt[2],urt[0],llb[1],urt[2],llb[0],llb[1],urt[2]);
					} else if(i==2) {
						rectStr = String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
								,llb[0],llb[1],llb[2],llb[0],urt[1],llb[2],llb[0],urt[1],urt[2],llb[0],llb[1],urt[2],llb[0],llb[1],llb[2]);
					} else if(i==3) {
						rectStr = String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
								,urt[0],llb[1],llb[2],urt[0],urt[1],llb[2],urt[0],urt[1],urt[2],urt[0],llb[1],urt[2],urt[0],llb[1],llb[2]);
					} else if(i==4) {
						rectStr = String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
								,llb[0],llb[1],llb[2],urt[0],llb[1],llb[2],urt[0],llb[1],urt[2],llb[0],llb[1],urt[2],llb[0],llb[1],llb[2]);
					} else if(i==5) {
						rectStr = String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
								,llb[0],urt[1],llb[2],urt[0],urt[1],llb[2],urt[0],urt[1],urt[2],llb[0],urt[1],urt[2],llb[0],urt[1],llb[2]);
					}
					rectStrs.add(rectStr);
				}
				return String.format("%s", join(", ",rectStrs));
			} else if (tmp.sameCount == 1) {
				if(tmp.xSame) {
					return String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
							, tmp.min[0], tmp.min[1], tmp.min[2]
									//반대로
							, tmp.min[0], tmp.min[1], tmp.max[2]
//							, tmp.min[0], tmp.max[1], tmp.min[2]
							, tmp.min[0], tmp.max[1], tmp.max[2]
									//여기도
							, tmp.min[0], tmp.max[1], tmp.min[2]
//							, tmp.min[0], tmp.min[1], tmp.max[2]
							, tmp.min[0], tmp.min[1], tmp.min[2]);
				} else if(tmp.ySame) {
					return String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
							, tmp.min[0], tmp.min[1], tmp.min[2]
									//반대로
							, tmp.min[0], tmp.min[1], tmp.max[2]
//							, tmp.max[0], tmp.min[1], tmp.min[2]
							, tmp.max[0], tmp.min[1], tmp.max[2]
									//여기도
							, tmp.max[0], tmp.min[1], tmp.min[2]
//							, tmp.min[0], tmp.min[1], tmp.max[2]
							, tmp.min[0], tmp.min[1], tmp.min[2]);
				} else if(tmp.zSame) {
					return String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f, %.1f %.1f %.1f))"
							, tmp.min[0], tmp.min[1], tmp.min[2]
									//반대로
							, tmp.max[0], tmp.min[1], tmp.min[2]
//							, tmp.min[0], tmp.max[1], tmp.min[2]
							, tmp.max[0], tmp.max[1], tmp.min[2]
									//여기도
							, tmp.min[0], tmp.max[1], tmp.min[2]
//							, tmp.max[0], tmp.min[1], tmp.min[2]
							, tmp.min[0], tmp.min[1], tmp.min[2]);
				}
			} else if (tmp.sameCount == 2) {
				return String.format("((%.1f %.1f %.1f, %.1f %.1f %.1f))"
						, tmp.min[0], tmp.min[1], tmp.min[2]
						, tmp.max[0], tmp.max[1], tmp.max[2]);
			} 
				
			return null;
		}
		
		public static OracleGeometry coordinates(OracleGeometry oGeometry) {
			double[] tmp = new double[oGeometry.dim];
			int c=0;
			for (int i=1;i<oGeometry.ordinates.length+1;i++){
				if(oGeometry.coords==null)
					oGeometry.coords = new ArrayList<double[]>();
					tmp[c++] = (oGeometry.ordinates[i-1]);
					if (i%oGeometry.dim == 0){
						oGeometry.coords.add(tmp);
						tmp = new double[oGeometry.dim];
						c=0;
					}
			}
			return oGeometry;
		}

		//region setCoordicates
		public static String setCoordicates(double[]... coordicates) {
			StringBuffer sb = new StringBuffer("");
			for(int i=0;i<coordicates.length;i++){
				for(int j=0;j<coordicates[i].length;j++){
					sb.append(coordicates[i][j]);
					if(j<coordicates[i].length-1) {
						sb.append(" ");
					}
				}
				if(i<coordicates.length-1) {
					sb.append(", ");
				}
			}
			return sb.toString();
		}
		
		public static String setCoordicates(List<double[]> coords,int start,int end) {
			
			StringBuffer str = new StringBuffer("");
			if(start==0) start = 1;
			if(end <= 0) {
				end = coords.size();
			}
			for(int i=start-1;i<end && (i<coords.size());i++) {
				double[] coord = coords.get(i);
				for(int j=0;j<coord.length;j++){
					str.append(coord[j]);
					if(j<coord.length-1)
						str.append(" ");
				}
				if(i<end-1&&i<coords.size()-1) {
					str.append(", ");
				}
			}
			return str.toString();
		}
		//endregion
		
		public static int eType (OracleGeometry oGeometry, int elemIndex) {
			if(elemIndex>=oGeometry.sdoElemInfoList.size()){
				return -1;
			}
			return oGeometry.sdoElemInfoList.get(elemIndex<0?0:elemIndex).SDO_ETYPE;
		}
		
		public static int interpretation (OracleGeometry oGeometry, int elemIndex) {
			if(elemIndex>=oGeometry.sdoElemInfoList.size()){
				return -1;
			}
			return oGeometry.sdoElemInfoList.get(elemIndex).SDO_INTERPRETATION;
		}
		
		public static int getStartOffset(OracleGeometry oGeometry, int elemIndex) {
			if(elemIndex>=oGeometry.sdoElemInfoList.size()){
				return -1;
			}
			return oGeometry.sdoElemInfoList.get(elemIndex).SDO_STARTING_OFFSET;
		}
		
		
		private static Map<Integer, Integer> pridMap = new HashMap<Integer, Integer>();
		public static int pridOraToPg(Connection oraConn, int oraPrid)
				throws SQLException {
			Integer pgPrid = pridMap.get(oraPrid);
			if (pgPrid == null) {
				String sql = "select sdo_cs.map_oracle_srid_to_epsg(" + oraPrid
						+ ") as prid from dual";
				PreparedStatement pstat = oraConn.prepareStatement(sql);
				ResultSet rs = pstat.executeQuery();
				rs.next();
				pgPrid = rs.getInt("PRID");
				if (pgPrid == 0)
					pgPrid = 4326;
				pridMap.put(oraPrid, pgPrid);
				rs.close();
				pstat.close();
			}
			return pgPrid;
		}
		
		public static List<SDO_ELEM_INFO> parseSdoElemInfo (int[] sdoElemInfo) {
			List<SDO_ELEM_INFO> sdoElemInfoList = null;
			if(sdoElemInfo!=null && sdoElemInfo.length>=3) {
				sdoElemInfoList = new ArrayList<SDO_ELEM_INFO>();
				for(int i=0;i<sdoElemInfo.length;i+=3){
					sdoElemInfoList.add(new SDO_ELEM_INFO(sdoElemInfo[i], sdoElemInfo[i+1], sdoElemInfo[i+2]));
				}
			}
			return sdoElemInfoList;
		}
		
		public static String join(String bindDelimiter,double[] dArr) {
			StringBuffer sb = new StringBuffer("");
			for(int i=0;i<dArr.length;i++){
				sb.append(dArr[i]);
				if(i<dArr.length-1){
					sb.append(bindDelimiter);
				}
			}
			return sb.toString();
		}
		public static String join(String bindDelimiter,List<String> strs) {
			StringBuffer sb = new StringBuffer("");
			for(int i=0;i<strs.size();i++){
				sb.append(strs.get(i));
				if(i<strs.size()-1){
					sb.append(bindDelimiter);
				}
			}
			return sb.toString();
		}
		
		public static String removeStr(String originStr,String removeStr) {
			String tmp = null;
			if(originStr.contains(removeStr)){
				tmp = originStr.replace(removeStr, "");
			}
			return tmp;
		}
		//endregion
		
}
