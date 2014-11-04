package com.bitcask.hdb.datafile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.bitcask.hdb.Hdb;
import com.bitcask.hdb.HdbConst;
import com.bitcask.hdb.common.Delegate1;
import com.bitcask.hdb.common.FileHelper;
import com.bitcask.hdb.config.HdbConfig;

/**
 * @author weijunbao
 * 
 * */
public final class DataFileManager {
	private volatile DataFile dataFile;
	/*是否正在生成新的 DataFile*/
	private AtomicBoolean doing=new AtomicBoolean(false);
	// Dummy value to associate with an Object in the backing Map
    private final Object PRESENT = new Object();
    
	private Map<Long,Object> olderDataFiles=new ConcurrentHashMap<Long,Object>();
	
	private Delegate1<Object> mergeFunc;
	public AtomicLong autoId;
	
	//前一个DataFileId
	public volatile long preDataFileId;
	
	public void setMergeFunc(Delegate1<Object> mergeFunc){
		this.mergeFunc=mergeFunc;
	}
	
	public List<Long> getOlderDataFiles(){
		List<Long> list=new ArrayList<Long>();
		Iterator<Long> it=olderDataFiles.keySet().iterator();
		while(it.hasNext()){
			list.add(it.next());
		}
		Collections.sort(list);
		return list;
	}
	
	public DataFile currentDataFile(){
		if(dataFile ==null)
			dataFile=new DataFile();
		else if(dataFile.getFileSize() > HdbConfig.getCurrent().getMaxDataFileSize())
			this.makeDataFile();
		return dataFile;
	}
	
	public void makeDataFile(){
		if(doing.get() == true)
			return;
		
		if(dataFile.getState())
			return;
		
		doing.set(true);
		DataFile oldDataFile=dataFile;
		
		try {
			oldDataFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
		
		olderDataFiles.put(dataFile.dataFileId,PRESENT);
		DataFileCounterManager.put(dataFile.dataFileId, dataFile.getDataFileCounter());
		
		dataFile=new DataFile();
		
		if(olderDataFiles.size() >= 5 && mergeFunc != null)
			mergeFunc.run(null);
		
		dataFile.setState(false);
		doing.set(false);
	}
	
	public void remove(long dataFileId){
		olderDataFiles.remove(dataFileId);
	}
	
	public void remove(Collection<Long> c){
		Iterator<Long> it=c.iterator();
		while(it.hasNext())
			olderDataFiles.remove(it.next());
	}
	
	public void recover(Hdb<?,?> hdb,String dataFiles[]){
		if(dataFiles != null && dataFiles.length > 0){
			String dataFileName;
			for(int i=0;i<= dataFiles.length -2;i++){
				dataFileName=dataFiles[i];
				this.olderDataFiles.put(Long.parseLong(dataFileName.substring(0, dataFileName.indexOf("."))), this.PRESENT);
			}
			dataFileName=dataFiles[dataFiles.length - 1];
			this.dataFile=new DataFile(Long.parseLong(dataFileName.substring(0, dataFileName.indexOf("."))));
			
			File f=new File(HdbConfig.getCurrent().getFilePath()+File.separator+dataFileName);
			if(f.exists() && f.isFile()){
				int filesize=(int)f.length();
				this.dataFile.setEstimateFileSize(filesize);
				this.dataFile.setFileSize(filesize);
			}
		}
	}
}
