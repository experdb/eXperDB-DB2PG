package com.k4m.experdb.db2pg.convert.pattern;

import java.util.regex.Pattern;

public abstract class SqlPattern {
	public interface MYSQL {
		Pattern CREATE_TABLE = Pattern.compile("^*(?i)CREATE TABLE$*");
		Pattern PRIMARY_KEY = Pattern.compile("^*(?i)PRIMARY KEY$*");
		Pattern UNIQUE_KEY = Pattern.compile("^*(?i)UNIQUE KEY$*");
		Pattern UNIQUE_INDEX = Pattern.compile("^*(?i)UNIQUE INDEX$*");
		Pattern KEY = Pattern.compile("^*(?i)KEY$*");
		Pattern INDEX = Pattern.compile("^*(?i)INDEX$*");
		Pattern FOREIGN_KEY = Pattern.compile("^*(?i)FOREIGN KEY$*");
		Pattern REFERENCES = Pattern.compile("^*(?i)REFERENCES$*");
		Pattern MATCH = Pattern.compile("^*(?i)MATCH$*");
		Pattern FULL = Pattern.compile("^*(?i)FULL$*");
		Pattern PARTIAL = Pattern.compile("^*(?i)PARTIAL$*");
		Pattern SIMPLE = Pattern.compile("^*(?i)SIMPLE$*");
		Pattern RESTRICT = Pattern.compile("^*(?i)RESTRICT$*");
		Pattern CASCADE = Pattern.compile("^*(?i)CASCADE$*");
		Pattern SET_NULL = Pattern.compile("^*(?i)SET NULL$*");
		Pattern NO_ACTION = Pattern.compile("^*(?i)NO ACTION$*");
		Pattern SET_DEFAULT = Pattern.compile("^*(?i)SET DEFAULT$*");
		Pattern ON_DELETE = Pattern.compile("^*(?i)ON DELETE$*");
		Pattern ON_UPDATE = Pattern.compile("^*(?i)ON UPDATE$*");
		Pattern NULL = Pattern.compile("^*(?i)NULL$*");
		Pattern NOT_NULL = Pattern.compile("^*(?i)NOT NULL$*");
		Pattern DEFAULT = Pattern.compile("^*(?i)DEFAULT$*");
		Pattern IF_NOT_EXISTS = Pattern.compile("^*(?i)IF NOT EXISTS$*");
		Pattern CONSTRAINT = Pattern.compile("^*(?i)CONSTRAINT$*");
		Pattern ASC = Pattern.compile("^*(?i)ASC$*");
		Pattern DESC = Pattern.compile("^*(?i)DESC$*");
		Pattern USING_BTREE = Pattern.compile("^*(?i)USING BTREE$*");
		Pattern USING_HASH = Pattern.compile("^*(?i)USING HASH$*");
		Pattern AUTO_INCREMENT = Pattern.compile("^*(?i)AUTO_INCREMENT$*");
		Pattern AUTO_INCREMENT2 = Pattern.compile("^*(?i)AUTO_INCREMENT\\s*=?\\s*\\d*$*");
		Pattern COMMENT = Pattern.compile("^*(?i)COMMENT$*");
		Pattern FULLTEXT = Pattern.compile("^*(?i)FULLTEXT$*");
		Pattern ENUM = Pattern.compile("^*(?i)ENUM$*");
	}
	public static boolean check(String str, java.util.regex.Pattern... patterns) {
		for(java.util.regex.Pattern pattern : patterns) {
			if(!pattern.matcher(str).find()) return false;
		}
		return true;
	}
}
