package com.k4m.experdb.db2pg.convert.map;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.k4m.experdb.db2pg.common.LogUtils;
import com.k4m.experdb.db2pg.config.ConfigInfo;
import com.k4m.experdb.db2pg.convert.ConvertObject;

public class SqlConvertMapper extends ConvertMapper<SqlConvertMapper> {
	
	protected SqlConvertMapper()  {
		try {
			init();
		} catch (FileNotFoundException e) {
			LogUtils.error("convert_map.json not found", SqlConvertMapper.class, e);
		} catch (IOException e) {
			LogUtils.error("io error", SqlConvertMapper.class, e);
		} catch (ParseException e) {
			LogUtils.error("json parse error", SqlConvertMapper.class, e);
		}
	}
	
	@Override
	protected void init() throws FileNotFoundException, IOException, ParseException {
		JSONParser jsonParser = new JSONParser();
		
		JSONObject convMapObj = (JSONObject)jsonParser.parse(new InputStreamReader(SqlConvertMapper.class.getResourceAsStream("/convert_map.json")));
		
		convertPatternValues = new ArrayList<ConvertObject>(30);
		convertDefaultValues = new ArrayList<ConvertObject>(5);
		for(Object key : convMapObj.keySet().toArray()) {
			JSONObject jobj = (JSONObject)convMapObj.get(key);
			String toValue = (String)jobj.get("postgres");
			JSONArray asValues = (JSONArray) jobj.get(ConfigInfo.SRC_DB_CONFIG.DB_TYPE);
			if(toValue != null && asValues != null) {
				for (Object asValue : asValues) {
					if(asValue instanceof String) {
						ConvertObject convVal = new ConvertObject((String)asValue,toValue);
						if(convVal.getPattern() != null) convertPatternValues.add(convVal);
						else convertDefaultValues.add(convVal);
					}
				}
			}
		}
	}
	@Override
	public List<ConvertObject> getDefaultList() {
		return convertDefaultValues;
	}

	@Override
	public List<ConvertObject> getPatternList() {
		return convertPatternValues;
	}

	@Override
	public SqlConvertMapper getMapper() {
		return this;
	}
	
}
