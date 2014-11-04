package com.bitcask.hdb.merge;

import java.io.BufferedInputStream;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bitcask.hdb.Hdb;
import com.bitcask.hdb.HdbConst;
import com.bitcask.hdb.HdbState;
import com.bitcask.hdb.StorageVaue;
import com.bitcask.hdb.common.BitConverter;
import com.bitcask.hdb.common.FileHelper;
import com.bitcask.hdb.common.Scope;
import com.bitcask.hdb.common.VCode;
import com.bitcask.hdb.config.HdbConfig;
import com.bitcask.hdb.datafile.DataFileCounter;
import com.bitcask.hdb.datafile.DataFileCounterManager;
import com.bitcask.hdb.datafile.DataFileHelper;
import com.bitcask.hdb.exception.MergeException;
import com.bitcask.hdb.exception.RecordParseException;
import com.bitcask.hdb.readfile.ReadFileManager;

/**
 * @author weijunbao
 * 
 * Merge规则
 * 1.按照文件ID从小到大一次Merge
 * 2.
 * */

public abstract class MergeStrategy {
	private static final Logger LOGGER = LoggerFactory.getLogger(MergeStrategy.class);
	
	protected Hdb<?,?> hdb;
	
	/**
	 * 目前只支持cache模式
	 * */
	private final boolean cacheMeshanism=true;
	private volatile boolean mergeing=false;
	
	private Map<Object,StorageVaue> mergeCache;
	
	private List<MergeCounter> mergeCounters = new ArrayList<MergeCounter>();
	
	public MergeStrategy(Hdb<?,?> hdb){
		this.hdb=hdb;
	}
	
	public void start(){
		throw new UnsupportedOperationException();
	}
	
	public void stop(){
		throw new UnsupportedOperationException();
	}
	
	/*
	 * 计算出Merge Id
	 * */
	public Scope<Long> getDataFileScope(List<Long> olderDataFiles){
		for(int index=olderDataFiles.size() -1;index>0;index--){
			long end=olderDataFiles.get(index);
			long start=olderDataFiles.get(index - 1);
			assert end > start;
			if(end-start -2 >= olderDataFiles.size()){
				return new Scope<Long>(start+1,end-1);
			}
		}
		return new Scope<Long>(olderDataFiles.get(0)-2 - olderDataFiles.size(),olderDataFiles.get(0)-1);
	}
	/*
	 * 根据文件数量和文件空间使用率,判断是否可以启动Merge
	 * */
	private boolean canMerge(List<Long> olderDataFiles){
		int spaceRate=0;
		int nullCount=0;
		if(olderDataFiles == null)
			return false;
		if(olderDataFiles.size() < HdbConst.MERGE_FILE_SIZE)
			return false;
		
		if(olderDataFiles.size() > HdbConst.FORCE_MERGE_FILE_SIZE)
			return true;
		
		for(int index=0;index<olderDataFiles.size();index++){
			DataFileCounter dataFileCounter=DataFileCounterManager.get(index);
			if(dataFileCounter != null)
				spaceRate+=dataFileCounter.SpaceRate();
			else
				nullCount++;
		}
		
		if(spaceRate/(olderDataFiles.size() - nullCount) > 0.5)
			return false;
		else
			return true;
	}

	
	/**
	 * 当merge文件超过设定大小时，判断是否用另起一个文件来写
	 * @param index
	 * @param filepos
	 * @param olderDataFiles
	 * @return
	 */
	private boolean canStartDataFile(int index,int filepos,List<Long> olderDataFiles){
		
		return false;
	}
	
	public void merge(boolean force){
		MergeCounter mergeCounter=null;
		boolean error=false;
		//merge 操作后产生的新的merge文件id
		List<Long> removeDataFiles=new ArrayList<Long>();
		List<Long> mergeDataFiles=new ArrayList<Long>();
		try{
			if(hdb.getHdbState() != HdbState.Running)
				return;
			
			if(this.mergeing)
				return;
			
			List<Long> olderDataFiles=hdb.getDataFileManager().getOlderDataFiles();
			if(force == false || canMerge(olderDataFiles) == false){
				return;
			}
			
			if(this.mergeing)
				return;
			
			this.mergeing=true;
			mergeCounter=new MergeCounter();
			mergeCounter.setStartTime(System.currentTimeMillis());
			
			Scope<Long> scope=getDataFileScope(olderDataFiles);
			//merge的时候用
			long mergeDataFileId=scope.getStart();
			if(cacheMeshanism && mergeCache != null)
				mergeCache=new HashMap<Object,StorageVaue>();
			
			//merge 文件
			FileOutputStream mergeOutput=new FileOutputStream(DataFileHelper.getDataFilePath(mergeDataFileId));
			FileDescriptor mergeFileDescriptor=mergeOutput.getFD();
			
			int writeValuePos=0;
			CRC32 crc32=new CRC32();
			//[start] 
			for(int index =0;index < olderDataFiles.size();index++){
				Long dataFileId=olderDataFiles.get(index);
				int currentPosition=0;
				//正在读的记录的开始位置
				int positionOfRecord=0;
				int positionOfValue=0;
				long crc,timestamp;
				int keySize,valueSize;
				Object keyObj=null;
				byte intBuffer[]=new byte[Integer.SIZE /8];
				byte longBuffer[]=new byte[Long.SIZE /8];
				byte keyBytes[]=null;
				byte valueBytes[]=null;
				BufferedInputStream dataFileInput = new BufferedInputStream(
						new FileInputStream(DataFileHelper.getDataFilePath(dataFileId)));
				//[start]
				while(true){
					crc32.reset();
					positionOfRecord=currentPosition;
					//read crc
					{
						if(dataFileInput.read(longBuffer) == -1){
							//read end
							mergeFileDescriptor.sync();
							dataFileInput.close();
							dataFileInput=null;
							
							if(FileHelper.delete(DataFileHelper.getDataFilePath(dataFileId)) == false){
								LOGGER.error("delete %s.%s error!",dataFileId,HdbConst.DATA_FILE_EXTENSION);
							}
							hdb.getDataFileManager().remove(dataFileId);
							removeDataFiles.add(dataFileId);
							break;
						}
					
						crc=BitConverter.getLong(longBuffer,0);
						currentPosition+=Long.SIZE / 8;
					}
					//read timestamp
					{
						if(dataFileInput.read(longBuffer) == -1){
							throw new MergeException("read timestamp error!",dataFileId,positionOfRecord);
						}
						
						timestamp=BitConverter.getLong(longBuffer,0);
						crc32.update(longBuffer);
						
						currentPosition+=Long.SIZE / 8;
					}
					
					//read keySize
					{
						if(dataFileInput.read(intBuffer) == -1){
							throw new MergeException("read keySize error!",dataFileId,positionOfRecord);
						}
						
						keySize=BitConverter.getInt(intBuffer,0);
						crc32.update(intBuffer);
						
						currentPosition+=Integer.SIZE / 8;
						
						if(keySize > HdbConst.MAX_KEY_SIZE)
							throw new MergeException(
									String.format("KeySize(%s) > MAX_KEY_SIZE",keySize),dataFileId,positionOfRecord);
					}
					
					//read valueSize
					{
						if(dataFileInput.read(intBuffer) == -1){
							throw new MergeException("read valueSize error!",dataFileId,positionOfRecord);
						}
						
						valueSize=BitConverter.getInt(intBuffer,0);
						crc32.update(intBuffer);
						
						currentPosition+=Integer.SIZE / 8;
						
						if(valueSize > HdbConst.MAX_VALUE_SIZE){
							throw new MergeException(
									String.format("valueSize(%s) > MAX_VALUE_SIZE",valueSize),dataFileId,positionOfRecord);
						}
					}
					//read key
					{
						keyBytes=new byte[keySize];
						if(dataFileInput.read(keyBytes) == -1){
							throw new MergeException("read key error!",dataFileId,positionOfRecord);
						}						
						keyObj=hdb.KeyFunc().get(keyBytes);
						crc32.update(keyBytes);
						
						currentPosition += keySize;
					}
					
					{
						StorageVaue storageValue=hdb.getStorageEngine().get(hdb.KeyFunc().get(keyBytes));
						if(storageValue == null){
							int loop=0;
							currentPosition += valueSize;
							while(true){
								long skipped=dataFileInput.skip(valueSize);
								if(loop >=10)
									throw new Exception(String.format("skip error,loop=%s",loop));
								
								loop++;
								if(skipped == valueSize)
									break;
								else
									valueSize -= skipped;
							}
							continue;
						}
						else{
							long dataFileIdOfRecord=VCode.getVCode(storageValue.getFileId(), 0, null);
							long valuePosOfRecord=VCode.getVCode(storageValue.getValuePos(), 0, null);
							long valueSizeOfRecord=VCode.getVCode(storageValue.getValueSize(), 0, null);
							
							//不是最新的数据
							if(dataFileIdOfRecord != dataFileId 
									|| valuePosOfRecord !=currentPosition 
									|| valueSizeOfRecord != valueSize){
								int loop=0;
								currentPosition += valueSize;
								while(true){
									long skipped=dataFileInput.skip(valueSize);
									if(loop >=10)
										throw new Exception(String.format("skip error,loop=%s",loop));
									
									loop++;
									if(skipped == valueSize)
										break;
									else
										valueSize -= skipped;
								}
								continue;
							}
						}
					}
					
					//read value
					{
						valueBytes=null;
						if(valueSize != 0){
							valueBytes=new byte[valueSize];
							if(dataFileInput.read(valueBytes) == -1){
								throw new MergeException("read valueSize error!",dataFileId,currentPosition);
							}
							crc32.update(valueBytes);
							
							currentPosition += valueSize;
						}
					}
					
 					if(crc32.getValue() != crc){
 						throw new RecordParseException("crc32 error",dataFileId,positionOfRecord);
 					}
 					
 					{
						//[start]  write to .merge file
 						mergeOutput.write(BitConverter.getBytes(crc));
						writeValuePos+=Long.SIZE / 8;
						
						mergeOutput.write(BitConverter.getBytes(timestamp));
						writeValuePos+=Long.SIZE / 8;
						
						mergeOutput.write(BitConverter.getBytes(keySize));
						writeValuePos+=Integer.SIZE / 8;
						
						mergeOutput.write(BitConverter.getBytes(valueSize));
						writeValuePos+=Integer.SIZE / 8;
						
						mergeOutput.write(keyBytes);
						writeValuePos+=keyBytes.length;
						
						positionOfValue=writeValuePos;
						if(valueBytes != null && valueBytes.length > 0){
							mergeOutput.write(valueBytes);
							writeValuePos+=valueBytes.length;
						}
						//[end]
						
						//
						if(writeValuePos > HdbConfig.getCurrent().getMaxDataFileSize() 
								&& canStartDataFile(0,0,null)){
							mergeDataFileId++;
							mergeOutput.flush();
							mergeOutput.close();
							writeValuePos=0;
							
							mergeOutput=new FileOutputStream(DataFileHelper.getDataFilePath(mergeDataFileId));
							mergeFileDescriptor=mergeOutput.getFD();
						}
					}
 					
					if(cacheMeshanism){
 						mergeCache.put(keyObj, 
 								new StorageVaue(
									VCode.getBytes(mergeDataFileId),
									VCode.getBytes(positionOfValue),
									VCode.getBytes(valueBytes.length))
 						);
 						
 						if(mergeCache.size() >= HdbConst.MERGE_CACHE_SIZE ){
 							//force flush
 							mergeFileDescriptor.sync();
 							
 							Iterator<Object> it=mergeCache.keySet().iterator();
 							synchronized (hdb.syncObject) {
	 							while(it.hasNext()){
	 								Object key=it.next();
	 								StorageVaue storageValue=mergeCache.get(key);
	 								hdb.innerPut(key, storageValue);
	 							}
 							}
 							mergeCache.clear();
 						}
					}
					else{
						mergeFileDescriptor.sync();
						hdb.innerPutWithLock(keyObj, new StorageVaue(
								VCode.getBytes(dataFileId),
								VCode.getBytes(positionOfValue),
								VCode.getBytes(valueBytes.length)));
					}
				}
				//[end] 
			}
			//[end]
			
			if(mergeCache.size() > 0){
				//force flush
				mergeFileDescriptor.sync();
				Iterator<Object> it=mergeCache.keySet().iterator();
				synchronized (hdb.syncObject) {
					while(it.hasNext()){
						Object key=it.next();
						StorageVaue storageValue=mergeCache.get(key);
						hdb.innerPut(key, storageValue);
					}
				}
				mergeCache.clear();
			}
			
			if(writeValuePos ==0){
				FileHelper.delete(DataFileHelper.getDataFilePath(mergeDataFileId));
			}
		}
		catch(RecordParseException ex){
			error=true;
			LOGGER.error("",ex);
		}
		catch(MergeException ex){
			error=true;
			LOGGER.error("",ex);
		}
		catch(Exception ex){
			error=true;
			LOGGER.error("merge",ex);
		}
		finally{
			if(mergeCounter != null){
				mergeCounter.setEndTime(System.currentTimeMillis());
				mergeCounter.setHasError(error);
			}
			//删除失效的
			Iterator<Long> it=removeDataFiles.iterator();
			while(it.hasNext()){
				ReadFileManager.del(it.next());
			}
			
			mergeCounters.add(mergeCounter);
			this.mergeing=false;
		}
	}
}
