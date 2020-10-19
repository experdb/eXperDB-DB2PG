package com.k4m.experdb.db2pg.convert.table;

import java.math.BigDecimal;

public class Sequence {
	private String seqName;
	private long seqStart;
	private long seqMinValue;
	private long seqIncValue;
	private BigDecimal seqMaxalue;
	
	public BigDecimal getSeqMaxalue() {
		return seqMaxalue;
	}
	public void setSeqMaxalue(BigDecimal seqMaxalue) {
		this.seqMaxalue = seqMaxalue;
	}
	public String getSeqName() {
		return seqName;
	}
	public void setSeqName(String seqName) {
		this.seqName = seqName;
	}
	public long getSeqStart() {
		return seqStart;
	}
	public void setSeqStart(long seqStart) {
		this.seqStart = seqStart;
	}
	public long getSeqMinValue() {
		return seqMinValue;
	}
	public void setSeqMinValue(long seqMinValue) {
		this.seqMinValue = seqMinValue;
	}
	public long getSeqIncValue() {
		return seqIncValue;
	}
	public void setSeqIncValue(long seqIncValue) {
		this.seqIncValue = seqIncValue;
	}

	@Override
	public String toString() {
		return "[seqName=" + seqName + ", seqStart=" + seqStart + ", seqMinValue=" + seqMinValue
				+ ", seqIncValue=" + seqIncValue+ ", seqMaxalue=" + seqMaxalue+ "]";
	}
}
