package com.k4m.experdb.db2pg.convert.table.key.option;

public class ReferenceDefinition {
	private ForeignKeyMatch match;
	private ForeignKeyDelete delete;
	private ForeignKeyUpdate update;
	
	public ReferenceDefinition() {
	}
	
	public ReferenceDefinition(ForeignKeyMatch match, ForeignKeyDelete delete, ForeignKeyUpdate update) {
		setMatch(match);
		setDelete(delete);
		setUpdate(update);
	}

	public ForeignKeyMatch getMatch() {
		return match;
	}

	public void setMatch(ForeignKeyMatch match) {
		this.match = match;
	}

	public ForeignKeyDelete getDelete() {
		return delete;
	}

	public void setDelete(ForeignKeyDelete delete) {
		this.delete = delete;
	}

	public ForeignKeyUpdate getUpdate() {
		return update;
	}

	public void setUpdate(ForeignKeyUpdate update) {
		this.update = update;
	}

	@Override
	public String toString() {
		return "ReferenceDefinition [match=" + match + ", delete=" + delete + ", update=" + update + "]";
	}
	
}