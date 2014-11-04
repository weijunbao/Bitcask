package com.bitcask.hdb.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.bitcask.hdb.common.StringHelper;

public final class HdbConfig{
	private Properties properties;
	private static HdbConfig hdbConfig;
	
	private HdbConfig() throws FileNotFoundException, IOException{
		properties=new Properties();
		properties.load(new FileInputStream("hdb.config"));
	}
	
	public static synchronized HdbConfig getCurrent(){
		if(hdbConfig == null){
			try {
				hdbConfig=new HdbConfig();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				hdbConfig=null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				hdbConfig=null;
			}
		}
		return hdbConfig;
	}
	
	public String getProperty(String key){
		if(properties != null)
			return properties.getProperty(key);
		else
			return null;
	}
	
	public String getFilePath(){
		String filepath="";
		if(properties != null)
			 filepath= properties.getProperty("file_path");
		
		if(StringHelper.isNullOrEmpty(filepath))
			return System.getProperty("user.dir");
		else
			return filepath;
	}
	
	public long getMaxDataFileSize(){
		String data_file_size=properties.getProperty("data_file_size");
		return Long.parseLong(data_file_size);
	}
}
