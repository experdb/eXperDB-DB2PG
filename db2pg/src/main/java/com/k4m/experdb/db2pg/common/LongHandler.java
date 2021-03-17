package com.k4m.experdb.db2pg.common;

import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
 
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
 
public class LongHandler implements TypeHandler{

	// 값을 받는 중요 값
	@Override
	public void setParameter(PreparedStatement ps, int i, Object parameter, JdbcType jdbcType) throws SQLException {
		String s = (String) parameter;
		StringReader reader = new StringReader(s);
		ps.setCharacterStream(i, reader, s.length());
	}
	
	// 이외 ~로 칼럼값 불러오는 메소드
	@Override
	public Object getResult(ResultSet rs, String columnName) throws SQLException {
		return rs.getString(columnName);
	}

	@Override
	public Object getResult(ResultSet arg0, int arg1) throws SQLException {
		return arg0.getObject(arg1);
	}
	
	@Override
	public Object getResult(CallableStatement arg0, int arg1) throws SQLException {
	// TODO Auto-generated method stub
		return arg0.getObject(arg1);
	}

}