package com.bitcask.hdb.common;

import java.io.File;

public final class Directory {
	
	public static boolean createDir(String filepath){
		File file=new File(filepath);
		file.mkdirs();
		if(file.exists())
			return true;
		else
			return false;
	}
	
	public static boolean isExists(String filepath){
		File file=new File(filepath);
		return file.exists();
	}
	
}
