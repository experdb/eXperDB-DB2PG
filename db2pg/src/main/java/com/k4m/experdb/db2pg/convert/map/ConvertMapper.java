package com.k4m.experdb.db2pg.convert.map;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.json.simple.parser.ParseException;

import com.k4m.experdb.db2pg.convert.ConvertObject;
import com.k4m.experdb.db2pg.convert.map.exception.MapperNotFoundException;

public abstract class ConvertMapper <T> {
	protected List<ConvertObject> convertDefaultValues;
	protected List<ConvertObject> convertPatternValues;
	
	public static ConvertMapper<?> makeConvertMapper(Class<?> clazz) throws MapperNotFoundException {
		if(clazz == MySqlConvertMapper.class) {
			return new MySqlConvertMapper();
		}
		throw new MapperNotFoundException();
	}
	
	protected abstract void init() throws FileNotFoundException, IOException, ParseException; 
	public abstract T getMapper();
	public abstract List<ConvertObject> getDefaultList();
	public abstract List<ConvertObject> getPatternList();
}
