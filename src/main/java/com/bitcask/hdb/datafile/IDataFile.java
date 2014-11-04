package com.bitcask.hdb.datafile;

import java.io.IOException;

import com.bitcask.hdb.StorageVaue;

public interface IDataFile {
	long getFileSize();
	/*
	 * @return the value's file position in the datafile
	 * */
	StorageVaue write(byte[] key,byte[] value) throws IOException;
	void flush(boolean force);
	void close() throws IOException;
}
