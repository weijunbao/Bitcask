package com.bitcask.hdb;

/**
 * @author weijunbao
 * 为了节约内存空间,所有字段都采用变长编码
 * */
public final class StorageVaue {
	byte[] fileId;
	byte[] vPos;
	byte[] vSize;
	
	public StorageVaue(byte[] fileId,byte[] vPos,byte[] vSize){
		this.fileId=fileId;
		this.vPos=vPos;
		this.vSize=vSize;
	}
	
	public byte[] getFileId(){
		return this.fileId;
	}
	
	public byte[] getValuePos(){
		return this.vPos;
	}
	
	public byte[] getValueSize(){
		return this.vSize;
	}
}
