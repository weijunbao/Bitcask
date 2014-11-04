package com.bitcask.hdb.datafile;

public final class HintFileRecord {
	long timestamp;
	int keySize;
	int valueSize;
	int valuePos;
	byte key[];
	
	public HintFileRecord(long timestamp,int keySize,int valueSize,int valuePos,byte key[]){
		this.timestamp=timestamp;this.keySize=keySize;
		this.valueSize=valueSize;this.valuePos=valuePos;
		this.key=key;
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

	public int getValuePos() {
		return valuePos;
	}

	public byte[] getKey() {
		return key;
	}
	
}
