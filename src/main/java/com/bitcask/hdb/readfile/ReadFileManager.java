package com.bitcask.hdb.readfile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bitcask.hdb.datafile.DataFileHelper;

/**
 * @author weijunbao
 * */
public final class ReadFileManager {
	private static Map<Long,RandomAccessFile> files=new ConcurrentHashMap<Long,RandomAccessFile>();
	
	public static RandomAccessFile get(long fileId){
		RandomAccessFile randomFile=files.get(fileId);
		if(randomFile == null){
			try {
				randomFile = new RandomAccessFile(DataFileHelper.getDataFilePath(fileId),"r");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				randomFile=null;
			}
			
			if(randomFile != null)
				files.put(fileId, randomFile);
		}
		return randomFile;
	}
	
	public static void del(long fileId){
		RandomAccessFile randomFile=files.remove(fileId);
		if(randomFile != null){
			try {
				randomFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
		}
	}
	
	public static void del(long... fileIds){
		for(long fileId : fileIds){
			RandomAccessFile randomFile=files.remove(fileId);
			if(randomFile != null){
				try {
					randomFile.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
