package com.k4m.experdb.db2pg.convert.table.key.option;

public enum ForeignKeyDelete {
	RESTRICT("ON DELETE RESTRICT"), CASCADE("ON DELETE CASCADE"), SET_NULL("ON DELETE SET NULL")
	, NO_ACTION("ON DELETE NO ACTION"), SET_DEFAULT("ON DELETE SET DEFAULT");
	final private String action;

	private ForeignKeyDelete(String action) {
		this.action = action;
	}

	public String getAction() {
		return action;
	}
}
