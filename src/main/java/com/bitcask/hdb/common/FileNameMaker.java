package com.bitcask.hdb.common;

import java.io.File;

import com.bitcask.hdb.HdbConst;
import com.bitcask.hdb.config.HdbConfig;

public final class FileNameMaker {
		
	private FileNameMaker(){}
	/**
	 * 
	 * */
	public static String getDataFileName(long fileId){
		//路径 前缀 autoId 后缀
		return String.format("%s%s%s%s%s", 
				HdbConfig.getCurrent().getFilePath(),
				File.separator,
				HdbConst.DATA_FILE_PREFIX,
				fileId,
				HdbConst.DATA_FILE_EXTENSION);
	}
	
	public static String getMergeFileName(int fileId){
		//filepath	/	fileId file extension
		return String.format("%s%s%s%s", HdbConfig.getCurrent().getFilePath(),File.separator,fileId,HdbConst.MERGE_FILE_EXTENSION);
	}
}
