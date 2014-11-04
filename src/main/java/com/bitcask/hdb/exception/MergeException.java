package com.bitcask.hdb.exception;

/**
 * @author weijunbao
 * */
public class MergeException extends Exception {
	
	private static final long serialVersionUID = 7483242526393016362L;
	
	private long dataFileId;
	private long positionOfRecord;
	
	public MergeException(String message,long dataFileId,long positionOfRecord){
		super(message);
		this.dataFileId=dataFileId;
		this.positionOfRecord=positionOfRecord;
	}
	
	public long getDataFileId(){
		return this.dataFileId;
	}
	
	public long getPositionOfRecord(){
		return this.positionOfRecord;
	}
	
	@Override
	public String toString(){
		return String.format("%s,DataFileId=%s,PositionOfRecord=%s",this.dataFileId,this.positionOfRecord);
	}
}
