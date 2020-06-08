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
import com.k4m.experdb.db2pg.db.datastructure.DBConfigInfo;

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
		//System.out.println("ConfigInfo.SRC_DB_CONFIG.DB_TYPE="+ConfigInfo.SRC_DB_CONFIG.DB_TYPE);
		JSONParser jsonParser = new JSONParser();
		JSONObject convMapObj = (JSONObject)jsonParser.parse(new InputStreamReader(SqlConvertMapper.class.getResourceAsStream("/convert_map.json")));
		convertPatternValues = new ArrayList<ConvertObject>(30);
		convertDefaultValues = new ArrayList<ConvertObject>(5);
		for(Object key : convMapObj.keySet().toArray()) {
			//System.out.println("key : "+key.toString());
			JSONObject jobj = (JSONObject)convMapObj.get(key);
			String toValue = (String)jobj.get("postgres");
			//System.out.println("postgres : "+toValue);
			JSONArray asValues = (JSONArray) jobj.get(ConfigInfo.SRC_DB_CONFIG.DB_TYPE.toLowerCase());
			//System.out.println("jobj : "+jobj);
			//System.out.println("ora asValues : "+asValues);
			if(toValue != null && asValues != null) {
				//System.out.println("ok 1");
				for (Object asValue : asValues) {
					//System.out.println("ok 2");
					if(asValue instanceof String) {
						//System.out.println("ok 3");
						ConvertObject convVal = new ConvertObject((String)asValue,toValue);
						//System.out.println("convVal : "+convVal);
						if(convVal.getPattern() != null) {
							convertPatternValues.add(convVal);
							//System.out.println("convVal 1");
						}
						else
						{
							convertDefaultValues.add(convVal);
							//System.out.println("convVal 2");
						}
					}
				}
			}
		}
	}
	@Override
	public List<ConvertObject> getDefaultList() {
		System.out.println("convertDefaultValues :"+convertDefaultValues);
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
