package com.k4m.experdb.db2pg.convert.parse;

import java.util.Arrays;

import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.map.exception.MapperNotFoundException;
import com.k4m.experdb.db2pg.db.datastructure.exception.DBTypeNotFoundException;

public abstract class StringParser <T> {
	public abstract T parse(String string, String compareValue) throws DBTypeNotFoundException;
	
	public static StringParser<?> makeParser(Class<?> clazz, ConvertMapper<?> convertMapper) throws MapperNotFoundException {
		if(clazz == TableStringParser.class) {
			return new TableStringParser(convertMapper);
		}
		throw new MapperNotFoundException();
	}
	
	
	public String findString(String original, String findKey, String regex) {
		int extendStart = findKey.indexOf(regex)+1;
		int start = original.indexOf(regex,original.indexOf(findKey)+extendStart)+1;
		int end = original.indexOf(regex,start+1);
		return original.substring(start,end);
	}
	
	/**
	 * <p>classify type = o(original) , u(upper), l(lower)</p>
	 * <p>classify default = original</p>
	 * */
	public String stringConvertClassify(String string, String classify,String...ignoreRangeCharacter) {
		StringBuffer sBuff = new StringBuffer(string);
		StringBuilder sBuilder = new StringBuilder("");
		int ignoreRangeCharacterIndex = 0;
		int[] ignoreRangeCharacterIndexes = new int[ignoreRangeCharacter.length]; 
		int start = 0;
		int end = -1, prevEnd = -1;
		System.out.println(Arrays.toString(ignoreRangeCharacter));
		System.out.println(string+"\n========================\n");
		do {
			ignoreRangeCharacterIndex = 0;
			for(int i=0;i<ignoreRangeCharacter.length;i++) {
				int idx = string.indexOf(ignoreRangeCharacter[i],prevEnd+1);
			}
			for(int i=1;i<ignoreRangeCharacter.length;i++) {
				if(ignoreRangeCharacterIndexes[ignoreRangeCharacterIndex]>ignoreRangeCharacterIndexes[i]) {
					ignoreRangeCharacterIndex = i;
				}
			}
			start = string.indexOf(ignoreRangeCharacter[ignoreRangeCharacterIndex],prevEnd+1);
			end = string.indexOf(ignoreRangeCharacter[ignoreRangeCharacterIndex],start+1)+1;
			if(start > -1 && end > -1) {
				switch(classify) {
				case "u": case "U":
					sBuilder.append(string.substring(prevEnd==-1?prevEnd+1:prevEnd,start).toUpperCase());
					break;
				case "l": case "L":
					sBuilder.append(string.substring(prevEnd==-1?prevEnd+1:prevEnd,start).toLowerCase());
					break;
				default :
					sBuilder.append(string.substring(prevEnd==-1?prevEnd+1:prevEnd,start));
				}
				
				sBuilder.append(string.substring(start,end));
			} else {
				break;
			}
			prevEnd = end;
		} while(start > -1 && end > -1);
		if(prevEnd<0) prevEnd=0;
		switch(classify) {
		case "u": case "U":
			sBuilder.append(sBuff.substring(prevEnd).toUpperCase());
			break;
		case "l": case "L":
			sBuilder.append(sBuff.substring(prevEnd).toLowerCase());
			break;
		default :
			sBuilder.append(sBuff.substring(prevEnd));
		}
		return sBuilder.toString();
	}

	
	
	
}
