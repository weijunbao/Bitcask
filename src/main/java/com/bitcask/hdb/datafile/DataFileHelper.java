package com.bitcask.hdb.datafile;

import java.io.File;

import com.bitcask.hdb.HdbConst;
import com.bitcask.hdb.config.HdbConfig;

public final class DataFileHelper {
	public static String getDataFilePath(long dataFileId){
		return HdbConfig.getCurrent().getFilePath() + File.separator + dataFileId +HdbConst.DATA_FILE_EXTENSION;
	}
	
	public static String getHintFilePath(long hintFileId){
		return HdbConfig.getCurrent().getFilePath() + File.separator + hintFileId +HdbConst.HINT_FILE_EXTENSION;
	}
}
