package com.k4m.experdb.db2pg.convert.table.key.option;

public enum ForeignKeyUpdate {
	RESTRICT("ON UPDATE RESTRICT"), CASCADE("ON UPDATE CASCADE"), SET_NULL("ON UPDATE SET NULL")
	, NO_ACTION("ON UPDATE NO ACTION"), SET_DEFAULT("ON UPDATE SET DEFAULT");
	final private String action;

	private ForeignKeyUpdate(String action) {
		this.action = action;
	}

	public String getAction() {
		return action;
	}
}
