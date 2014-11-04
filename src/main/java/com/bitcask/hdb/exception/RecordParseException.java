package com.bitcask.hdb.exception;

/**
 * @author weijunbao
 *
 * */
public class RecordParseException extends Exception {
	private static final long serialVersionUID = -2421706997660738717L;
	
	private long dataFileId;
	private long positionOfRecord;
	
	public RecordParseException(String message,long dataFileId,long positionOfRecord){
		super(message);
		this.dataFileId=dataFileId;
		this.positionOfRecord=positionOfRecord;
	}
	
	@Override
	public String toString(){
		return String.format("%s,%s,%s", this.getMessage(),this.dataFileId,this.positionOfRecord);
	}
}
