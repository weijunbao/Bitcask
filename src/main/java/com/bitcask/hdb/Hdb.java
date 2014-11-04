package com.bitcask.hdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bitcask.hdb.common.BitConverter;
import com.bitcask.hdb.common.Directory;
import com.bitcask.hdb.common.FileHelper;
import com.bitcask.hdb.common.VCode;
import com.bitcask.hdb.config.HdbConfig;
import com.bitcask.hdb.datafile.DataFile;
import com.bitcask.hdb.datafile.DataFileManager;
import com.bitcask.hdb.datafile.DataFileReader;
import com.bitcask.hdb.datafile.DataFileRecord;
import com.bitcask.hdb.datafile.HintFileReader;
import com.bitcask.hdb.datafile.HintFileRecord;
import com.bitcask.hdb.merge.MergeStrategy;
import com.bitcask.hdb.merge.MergeStrategyFactory;
import com.bitcask.hdb.readfile.ReadFileManager;

/**
 * @author weijunbao
 *
 */
public final class Hdb<K,V> implements IDbOperate<K,V>{
	private static final Logger LOGGER = LoggerFactory.getLogger(Hdb.class); 
	
	Map<K,StorageVaue> storageEngine;
	DataFileManager dataFileManager;
	ReadFileManager readFileManager;
	//支持的最多记录数
	int maxsize=1000;
	final IByte<K> keyFunc;
	final IByte<V> valueFunc;
	MergeStrategy mergeStrategy;
	AtomicReference<HdbState> hdbState;
	final long START_TIME_STAMP=0L;
	public Object syncObject=new Object();
	public static final long startTime=System.currentTimeMillis();//为了减少timestamp的占用内存量
	
	public Hdb(IByte<K> keyfunc,IByte<V> valuefunc) throws FileNotFoundException, IOException{
		storageEngine = new HashMap<K,StorageVaue>(this.maxsize);
		dataFileManager=new DataFileManager();
		readFileManager=new ReadFileManager();
		keyFunc=keyfunc;
		valueFunc=valuefunc;
		mergeStrategy=MergeStrategyFactory.createMergeStrategy(this);
		mergeStrategy.start();
		hdbState=new AtomicReference<HdbState>();
		//create db path
		FileHelper.createFile(HdbConfig.getCurrent().getFilePath());
	}
	
	public HdbState getHdbState(){
		return hdbState.get();
	}
	
	public void setHdbState(HdbState hdbState){
		this.hdbState.set(hdbState);
	}
	
	public IByte<K> KeyFunc(){
		return this.keyFunc;
	}
	
	public IByte<V> ValueFunc(){
		return this.valueFunc;
	}
	
	public Map<K,StorageVaue> getStorageEngine(){
		return this.storageEngine;
	}
	
	public DataFileManager getDataFileManager(){
		return this.dataFileManager;
	}
	
	public void innerPut(Object key,StorageVaue storageValue){
		this.storageEngine.put((K)key, storageValue);
	}
	
	public void innerPutWithLock(Object key,StorageVaue storageValue){
		synchronized (this.syncObject) {
			this.storageEngine.put((K)key, storageValue);
		}
	}
	
	public void forceMerge(){
		this.mergeStrategy.start();
	}
	
	public void open() {
		// TODO Auto-generated method stub
		String filepath=HdbConfig.getCurrent().getFilePath();
		if(Directory.isExists(filepath)){
			this.recover();
		}
	}

	@SuppressWarnings("null")
	public V get(K key) {
		// TODO Auto-generated method stub
		if(hdbState.get() != HdbState.Running)
			return null;
		
		StorageVaue storageValue=storageEngine.get(key);
		
		if(storageValue != null){
			long dataFileId=VCode.getVCode(storageValue.getFileId(),0,null);
			RandomAccessFile randomFile=ReadFileManager.get(dataFileId);
			if(randomFile != null){
				int valueSize=(int) VCode.getVCode(storageValue.getValueSize(),0,null);
				byte valueBody[]=new byte[valueSize];
				try {
					long pos=VCode.getVCode(storageValue.getValuePos(),0,null);
					randomFile.seek(pos);
					if(randomFile.read(valueBody) == -1){
						//force flush
						DataFile dataFile=this.dataFileManager.currentDataFile();
						if(dataFile.getDataFileId() == dataFileId){
							dataFile.flush(true);
						}
						
						if(randomFile.read(valueBody) == -1){
							LOGGER.error("hdb.get()","");
							return null;
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOGGER.error("hdb.get()",e);
				}
				return this.valueFunc.get(valueBody, 0, (int)VCode.getVCode(storageValue.getValueSize(),0,null));
			}
		}
		return null;
	}
	
	public List<V> getBatch(List<K> keys){
		if(hdbState.get() != HdbState.Running)
			return null;
		
		return null;
	}

	public void put(K key, V value) throws Exception {
		// TODO Auto-generated method stub
		if(hdbState.get() != HdbState.Running)
			return;
		
		if(key == null || value == null)
			return;
		
		StorageVaue storageValue=null;
		byte keyBytes[]=keyFunc.toByte(key);
		byte valueBytes[]=valueFunc.toByte(value);
		
		storageValue=this.storageEngine.get(key);
		
		synchronized (syncObject) {
			DataFile dataFile=dataFileManager.currentDataFile();
			storageValue=dataFile.write(keyBytes, valueBytes);
			
			this.storageEngine.put(key, storageValue);
		}
	}

	public void putBatch(List<K> keys,List<V> values) throws IOException{
		if(hdbState.get() != HdbState.Running)
			return;
		
		if(keys ==null ||  values == null )
			return;
		
		if(keys.size() != 0 && (keys.size() != values.size()))
			return;
		
		synchronized (syncObject) {
			for(int i=0;i<keys.size();i++){
				K key=keys.get(i);
				V value=values.get(i);
				StorageVaue storageValue=null;
				byte keyBytes[]=keyFunc.toByte(key);
				byte valueBytes[]=valueFunc.toByte(value);
				
				storageValue=this.storageEngine.get(key);
				
				DataFile dataFile=dataFileManager.currentDataFile();
				
				storageValue=dataFile.write(keyBytes, valueBytes);
				
				this.storageEngine.put(key, storageValue);
			}
		}
	}
	
	public void remove(K key) throws IOException {
		// TODO Auto-generated method stub
		if(hdbState.get() != HdbState.Running)
			return;
		
		if(key == null)
			return;
		StorageVaue storageValue=this.storageEngine.get(key);
		if(storageValue != null)
			synchronized (syncObject) {
				storageValue =dataFileManager.currentDataFile().write(keyFunc.toByte(key), null);
				storageEngine.remove(key);
			}
	}
	
	public void removeBatch(List<K> keys) throws IOException{
		if(hdbState.get() != HdbState.Running)
			return;
		
		if(keys == null || keys.size() == 0)
			return;
		
		for(K key : keys){
			if(key == null)
				return;
		}
		
		synchronized (syncObject) {
			for(K key : keys){
				DataFile dataFile=dataFileManager.currentDataFile();
				dataFile.write(keyFunc.toByte(key), null);
			}
			
			for(K key : keys)
				storageEngine.remove(key);
		}
	}
	
	public void recover() {
		// TODO Auto-generated method stub
		hdbState.set(HdbState.Recover);
		
		try{
			String dataFiles[]=FileHelper.list(HdbConfig.getCurrent().getFilePath(), HdbConst.DATA_FILE_EXTENSION);
			String hintFiles[]=FileHelper.list(HdbConfig.getCurrent().getFilePath(), HdbConst.HINT_FILE_EXTENSION);
			
			 Arrays.sort(dataFiles,new Comparator<String>(){
	
				public int compare(String str1, String str2) {
					// TODO Auto-generated method stub
					long a1=Long.parseLong(str1.substring(0,str1.indexOf(".")));
					long a2=Long.parseLong(str2.substring(0,str2.indexOf(".")));
					return (int)(a1-a2);
				}}
			 );
			 
			 Arrays.sort(hintFiles,new Comparator<String>(){
	
				public int compare(String str1, String str2) {
					// TODO Auto-generated method stub
					long a1=Long.parseLong(str1.substring(0,str1.indexOf(".")));
					long a2=Long.parseLong(str2.substring(0,str2.indexOf(".")));
					return (int)(a1-a2);
				}}
			 );
			 
			 //begin verify
			 assert (dataFiles!=null ? dataFiles.length : 0) >= (hintFiles!=null ? hintFiles.length : 0);
			 
			 for(int i=0;i<hintFiles.length;i++){
				 String hintFile=hintFiles[i];
				 String dataFile=dataFiles[i];
				 
				 for(int index=0;index<hintFile.length() && index < dataFile.length();index++){
					 char ch1=hintFile.charAt(index);
					 char ch2=dataFile.charAt(index);
					 assert ch1==ch2;
					 if(ch1=='.')
						 break;
				 }
			 }
			 //end verify
			 
			if(hintFiles != null){
				for(int i=0;i < hintFiles.length;i++){
					String hintFile=hintFiles[i];
					String hintFileId=hintFile.substring(0,hintFile.indexOf("."));
					
					HintFileRecord hintFileRecord;
					HintFileReader hintFileReader = null;
					try {
						hintFileReader = new HintFileReader(HdbConfig.getCurrent().getFilePath() + File.separator + hintFile);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						
					}
					while((hintFileRecord = hintFileReader.readNext()) != null){
						storageEngine.put(
							keyFunc.get(hintFileRecord.getKey()), 
							new StorageVaue(
								VCode.getBytes(Long.parseLong(hintFileId)),
								VCode.getBytes(hintFileRecord.getValuePos()),
								VCode.getBytes(hintFileRecord.getValueSize()))
							);
					}
				}
			}
			
			for(int i=hintFiles==null ? 0 :hintFiles.length;i<dataFiles.length;i++){
				String dataFile=dataFiles[i];
				DataFileRecord dataFileRecord;
				String dataFileId=dataFile.substring(0,dataFile.indexOf("."));
				long _dataFileId=Long.parseLong(dataFileId);
				
				DataFileReader dataFileReader = null;
				try {
					dataFileReader = new DataFileReader(HdbConfig.getCurrent().getFilePath()+File.separator+dataFile);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
				}
				
				while((dataFileRecord=dataFileReader.readNext()) != null){
					if(dataFileRecord.getValueSize() == 0)
						this.storageEngine.remove(keyFunc.get(dataFileRecord.getKey()));
					else
						this.storageEngine.put(keyFunc.get(dataFileRecord.getKey()), new StorageVaue(
								VCode.getBytes(_dataFileId),
								VCode.getBytes(dataFileRecord.getValuePos()),
								VCode.getBytes(dataFileRecord.getValueSize())));
				}
				
				if(dataFileReader != null){
					dataFileReader.close();
				}
			}
			
			
			dataFileManager.recover(this,dataFiles);
			FileHelper.delete(HdbConfig.getCurrent().getFilePath()+File.separator+HdbConst.OK_FILE);
			
			//强行merge
			if(dataFiles.length > HdbConst.MERGE_FILE_SIZE)
				this.mergeStrategy.merge(true);
			
			hdbState.set(HdbState.Running);
		}
		catch(Exception ex){
			LOGGER.error("hdb.recover",ex);
			hdbState.set(HdbState.Stoped);
		}
		finally{
			
		}
	}

	public void close() throws Exception {
		// TODO Auto-generated method stub
		hdbState.set(HdbState.Closing);
		//延迟关闭
		Thread.sleep(10000);
		
		String okfile=HdbConfig.getCurrent().getFilePath() + File.separator + HdbConst.OK_FILE;
		
		if(FileHelper.createFile(okfile)){
			FileOutputStream output=new FileOutputStream(okfile);
			output.write(this.storageEngine.size());
			output.write(BitConverter.getBytes(0xef123456));
			output.flush();
			output.close();
			output=null;
		}
		dataFileManager.currentDataFile().close();
		
		storageEngine = null;
		dataFileManager=null;
		readFileManager=null;
		
		mergeStrategy=null;
		hdbState.set(HdbState.Stoped);
	}
	
}
