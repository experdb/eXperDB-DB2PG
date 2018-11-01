package com.k4m.experdb.db2pg.writer;

public class WriterVO {
	private int processLines; //FILE&DB 된 총 로우 수  FileWriter는 INSERT 된 로우 수를 알 수 없으므로 0
	private int processBytes; //FILE&DB Writer가 처리한 총 Bytes 수
	private int porcessErrorLines; //FILE&DB가 재처리를 위해 제외한 로우 수 (적재가 실패한 총 로우수)

	public int getProcessLines() {
		return processLines;
	}

	public void setProcessLines(int processLines) {
		this.processLines = processLines;
	}

	public int getProcessBytes() {
		return processBytes;
	}

	public void setProcessBytes(int processBytes) {
		this.processBytes = processBytes;
	}

	public int getPorcessErrorLines() {
		return porcessErrorLines;
	}

	public void setPorcessErrorLines(int porcessErrorLines) {
		this.porcessErrorLines = porcessErrorLines;
	}

}
