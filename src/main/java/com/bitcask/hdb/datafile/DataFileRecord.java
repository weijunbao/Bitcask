package com.bitcask.hdb.datafile;

public final class DataFileRecord {
	private long crc;
	private long timestamp;
	private int keySize;
	private int valueSize;
	private byte key[];
	private byte value[];
	private int valuePos;
	
	public DataFileRecord(long crc,long timestamp,int keySize,int valueSize,byte key[],byte value[],int valuePos){
		this.crc=crc;this.timestamp=timestamp;
		this.keySize=keySize;this.valueSize=valueSize;
		this.key=key;this.value=value;
		this.valuePos=valuePos;
	}
	
	public long getCrc() {
		return crc;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getKeySize() {
		return keySize;
	}
	
	public int getValueSize() {
		return valueSize;
	}
	
	public byte[] getKey() {
		return key;
	}
	
	public byte[] getValue() {
		return value;
	}
	
	public int getValuePos() {
		return valuePos;
	}
}
