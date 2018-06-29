package com.k4m.experdb.db2pg.convert.parse;

import java.util.ArrayList;
import java.util.List;

import com.k4m.experdb.db2pg.common.Constant;
import com.k4m.experdb.db2pg.convert.map.ConvertMapper;
import com.k4m.experdb.db2pg.convert.pattern.SqlPattern;
import com.k4m.experdb.db2pg.convert.table.key.ForeignKey;
import com.k4m.experdb.db2pg.convert.table.key.Key;
import com.k4m.experdb.db2pg.convert.table.key.NormalKey;
import com.k4m.experdb.db2pg.convert.table.key.PrimaryKey;
import com.k4m.experdb.db2pg.convert.table.key.UniqueKey;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyDelete;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyMatch;
import com.k4m.experdb.db2pg.convert.table.key.option.ForeignKeyUpdate;
import com.k4m.experdb.db2pg.convert.table.key.option.ReferenceDefinition;
import com.k4m.experdb.db2pg.db.datastructure.exception.DBTypeNotFoundException;

public class KeyParser extends StringParser<Key<?>> {
	private ConvertMapper<?> convertMapper;
		
	public KeyParser(ConvertMapper<?> convertMapper) {
		this.convertMapper = convertMapper;
	}
	
	@Override
	public Key<?> parse(String ddlString, String compareValue) throws DBTypeNotFoundException {
		return getKey(ddlString,compareValue);
	}
	
	private Key<?> getKey (String ddlString, String compareValue) throws DBTypeNotFoundException {
		switch(compareValue) {
		case Constant.DB_TYPE.MYSQL : 
			return getKeyMysqlToPog(ddlString);
		default :
			throw new DBTypeNotFoundException();
		}
	}
	private Key<?> getKeyMysqlToPog (String ddlString) throws DBTypeNotFoundException {
		Key<?> key = null;
		if(SqlPattern.check(ddlString,SqlPattern.MYSQL.PRIMARY_KEY)) {
			PrimaryKey pkey = new PrimaryKey();
			
			pkey.setColumns(getValues(ddlString,"PRIMARY KEY", "(", ")"));
			
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_BTREE)) {
				pkey.setIndexType(Key.IndexType.BTREE);
			} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_HASH)) {
				pkey.setIndexType(Key.IndexType.HASH);
			}
			key = pkey;
		} else if (SqlPattern.check(ddlString,SqlPattern.MYSQL.UNIQUE_KEY)) {
			UniqueKey ukey = new UniqueKey();
			ukey.setColumns(getValues(ddlString,"UNIQUE KEY", "(", ")"));
			
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_BTREE)) {
				ukey.setIndexType(Key.IndexType.BTREE);
			} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_HASH)) {
				ukey.setIndexType(Key.IndexType.HASH);
			}
			if(ddlString.substring(0, ddlString.indexOf("(")).indexOf("\"") != -1) {
				ukey.setName(getValue(ddlString,"UNIQUE KEY", "\"", "\""));
			}
			key = ukey;
		} else if (SqlPattern.check(ddlString, SqlPattern.MYSQL.UNIQUE_INDEX)) {
			UniqueKey ukey = new UniqueKey();
			ukey.setColumns(getValues(ddlString,"UNIQUE INDEX", "(", ")"));
			
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_BTREE)) {
				ukey.setIndexType(Key.IndexType.BTREE);
			} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_HASH)) {
				ukey.setIndexType(Key.IndexType.HASH);
			}
			if(ddlString.substring(0, ddlString.indexOf("(")).indexOf("\"") != -1) {
				ukey.setName(getValue(ddlString,"UNIQUE INDEX", "\"", "\""));
			}
			key = ukey;
		} else if ( SqlPattern.check(ddlString, SqlPattern.MYSQL.FOREIGN_KEY)) {
			ForeignKey fkey = new ForeignKey();
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.CONSTRAINT)) {
				if(ddlString.substring(0, ddlString.indexOf("(")).indexOf("\"") != -1) {
					fkey.setName(getValue(ddlString,"CONSTRAINT", "\"", "\""));
				}
			}
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.FOREIGN_KEY)) {
				fkey.setColumns(getValues(ddlString,"FOREIGN KEY", "(", ")"));
			}
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.REFERENCES)) {
				fkey.setRefTable(getValue(ddlString,"REFERENCES", "\"", "\""));
				fkey.setRefColumns(getValues(ddlString,"REFERENCES", "(", ")"));
				ReferenceDefinition refDef = new ReferenceDefinition();
				fkey.setRefDef(refDef);
				if(SqlPattern.check(ddlString, SqlPattern.MYSQL.MATCH)) {
					if(SqlPattern.check(ddlString, SqlPattern.MYSQL.FULL)) {
						refDef.setMatch(ForeignKeyMatch.FULL);
					} else if (SqlPattern.check(ddlString, SqlPattern.MYSQL.PARTIAL)) {
						refDef.setMatch(ForeignKeyMatch.PARTIAL);
					} else if (SqlPattern.check(ddlString, SqlPattern.MYSQL.SIMPLE)) {
						refDef.setMatch(ForeignKeyMatch.SIMPLE);
					}
				}
				
				if(SqlPattern.check(ddlString, SqlPattern.MYSQL.ON_DELETE)) {
					if(SqlPattern.check(ddlString, SqlPattern.MYSQL.NO_ACTION)) {
						refDef.setDelete(ForeignKeyDelete.NO_ACTION);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.RESTRICT)) {
						refDef.setDelete(ForeignKeyDelete.RESTRICT);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.CASCADE)) {
						refDef.setDelete(ForeignKeyDelete.CASCADE);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.SET_NULL)) {
						refDef.setDelete(ForeignKeyDelete.SET_NULL);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.SET_DEFAULT)) {
						refDef.setDelete(ForeignKeyDelete.SET_DEFAULT);
					}
				}
				
				if(SqlPattern.check(ddlString, SqlPattern.MYSQL.ON_UPDATE)) {
					if(SqlPattern.check(ddlString, SqlPattern.MYSQL.NO_ACTION)) {
						refDef.setUpdate(ForeignKeyUpdate.NO_ACTION);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.RESTRICT)) {
						refDef.setUpdate(ForeignKeyUpdate.RESTRICT);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.CASCADE)) {
						refDef.setUpdate(ForeignKeyUpdate.CASCADE);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.SET_NULL)) {
						refDef.setUpdate(ForeignKeyUpdate.SET_NULL);
					} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.SET_DEFAULT)) {
						refDef.setUpdate(ForeignKeyUpdate.SET_DEFAULT);
					}
				}
				
			}
			
			key = fkey;
		} else if ( SqlPattern.check(ddlString, SqlPattern.MYSQL.KEY)) {
			NormalKey nkey = new NormalKey();
			nkey.setColumns(getValues(ddlString,"KEY", "(", ")"));
			
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_BTREE)) {
				nkey.setIndexType(Key.IndexType.BTREE);
			} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_HASH)) {
				nkey.setIndexType(Key.IndexType.HASH);
			}
			if(ddlString.substring(0, ddlString.indexOf("(")).indexOf("\"") != -1) {
				nkey.setName(getValue(ddlString,"KEY", "\"", "\""));
			}
			key = nkey;
		} else if( SqlPattern.check(ddlString, SqlPattern.MYSQL.INDEX)) {
			NormalKey nkey = new NormalKey();
			
			nkey.setColumns(getValues(ddlString,"INDEX", "(", ")"));
			
			if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_BTREE)) {
				nkey.setIndexType(Key.IndexType.BTREE);
			} else if(SqlPattern.check(ddlString, SqlPattern.MYSQL.USING_HASH)) {
				nkey.setIndexType(Key.IndexType.HASH);
			}
			
			if(ddlString.substring(0, ddlString.indexOf("(")).indexOf("\"") != -1) {
				nkey.setName(getValue(ddlString,"INDEX", "\"", "\""));
			}
			
			key = nkey;
		}
		return key;
	}
	
	private String getValue(String str, String findKey, String sKey, String eKey) {
		int start = str.indexOf(sKey,str.indexOf(findKey)+findKey.length())+1;
		int end = str.indexOf(eKey,start+1);
		return str.substring(start,end).replace("\"", "").trim();
	}
	
	private List<String> getValues(String str, String findKey, String sKey, String eKey) {
		int start = str.indexOf(sKey,str.indexOf(findKey)+findKey.length())+1;
		int end = str.indexOf(eKey,start+1);
		
		List<String> list = new ArrayList<String>();
		for(String value : str.substring(start,end).replace("\"", "").split(",")) {
			list.add(value.trim());
		}
		return list;
	}
}
