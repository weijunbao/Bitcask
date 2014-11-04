package com.bitcask.hdb.common;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class FileHelper {
	
	private static void makeDir(File dir){
		if(!dir.getParentFile().exists()){
			makeDir(dir.getParentFile());
		}
		dir.mkdir();
	}
	
	public static boolean createFile(String filepath){
		File file=new File(filepath);
		if(!file.exists())
			makeDir(file.getParentFile());
		
		try {
			return file.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return false;
		}
	}

	public static String[] list(String dir,final String extension){
		File file=new File(dir);
		if(!file.exists())
			return null;
		
		if(!file.isDirectory())
			return null;
		
		return file.list(new FilenameFilter(){

			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				if(name.endsWith(extension))
					return true;
				else
					return false;
			}}
		);
	}
	
	public static boolean delete(String name){
		File file=new File(name);
		if(file.exists())
			return file.delete();
		return true;
	}
	
	public static List<Boolean> delete(String... names){
		List<Boolean> delIndex = null;
		if(names.length > 0){
			delIndex=new ArrayList<Boolean>();
			for(String name : names){
				delIndex.add(delete(name));
			}
		}
		return delIndex;
	}
}
