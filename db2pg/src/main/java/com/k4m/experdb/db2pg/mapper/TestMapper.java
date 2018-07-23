package com.k4m.experdb.db2pg.mapper;

import java.util.List;
import java.util.Map;

public interface TestMapper {
	public List<Map<String,Object>> test();
	public List<?> provider_test();
}
