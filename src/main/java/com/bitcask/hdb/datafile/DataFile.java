package com.bitcask.hdb.datafile;

import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import com.bitcask.hdb.StorageVaue;
import com.bitcask.hdb.common.BitConverter;
import com.bitcask.hdb.common.VCode;

/**
 * @author weijunbao
 *
 * DataFileId 必须升序，且唯一
 */
public class DataFile implements IDataFile{
	FileOutputStream output;
	FileDescriptor fileDescriptor;
	//unique
	final long dataFileId;

	int fileSize=0;
	int estimateFileSize=0;
	//是否已经产生了新的DataFile
	AtomicBoolean state=new AtomicBoolean(false);
	AtomicBoolean atomicBoolean=new AtomicBoolean(false); 
	
	DataFileCounter dataFileCounter;
	Timer timer=new Timer();
	CRC32 crc32=new CRC32();

	public DataFile() {
		this(System.currentTimeMillis());
	}
	
	public DataFile(long dataFileId){
		this.dataFileId=dataFileId;
		try {
			output=new FileOutputStream(DataFileHelper.getDataFilePath(dataFileId),true);
			this.fileDescriptor=output.getFD();
		} catch (FileNotFoundException e) {
			output=null;
		} catch (IOException e) {
			
		}
		dataFileCounter=new DataFileCounter();
		timer.schedule(new ForceFlush(), (long) (1000 * 1.5),(long)(1000*1.5));
	}
	
	public long getFileSize(){
		return this.fileSize;
	}
	
	public void setFileSize(int filesize){
		this.fileSize=filesize;
	}
	
	public int estimateFileSize(){
		return this.estimateFileSize;
	}
	
	public DataFileCounter getDataFileCounter(){
		return this.dataFileCounter;
	}
	
	public void setEstimateFileSize(int estimateFileSize){
		this.estimateFileSize=estimateFileSize;
	}
	
	public long getDataFileId(){
		return this.dataFileId;
	}
	
	public void setState(boolean b){
		state.set(b);
	}
	
	public boolean getState(){
		return state.get();
	}
	
	/**
	 * @param key
	 * @param value
	 * 		delete operate,if value is null
	 * @param oldValueSize
	 * 	
	 * */
	public StorageVaue write(byte[] key, byte[] value) throws IOException {
		// TODO Auto-generated method stub
		int valuePos;
		long timestamp=System.currentTimeMillis();
		byte timestampByte[]=BitConverter.getBytes(timestamp );
		byte keySizeByte[]=BitConverter.getBytes(key.length);
		byte valueSizeByte[]=BitConverter.getBytes(value == null ? 0 :value.length);
		
		//calculate crc
		crc32.reset();
		crc32.update(timestampByte);
		
		crc32.update(keySizeByte);
		crc32.update(valueSizeByte);
		crc32.update(key);
		
		if(value != null)
			crc32.update(value);
		
		//write crc
		output.write(BitConverter.getBytes(crc32.getValue()));
		estimateFileSize += Long.SIZE / 8;
		
		//write timestamp
		output.write(timestampByte);
		estimateFileSize += timestampByte.length;
		
		//write keySize
		output.write(keySizeByte);
		estimateFileSize += keySizeByte.length;
		
		//write valueSize
		if(value == null || value.length == 0)
			output.write(BitConverter.getBytes(0));
		else
			output.write(valueSizeByte);
		
		estimateFileSize += Integer.SIZE / 8;
		
		//write key
		output.write(key);
		estimateFileSize += key.length;
		valuePos=estimateFileSize;
		//write value
		if(value != null && value.length > 0){
			output.write(value);
			estimateFileSize += value.length;
		}
		
		{
			dataFileCounter.total++;
			dataFileCounter.keySpace+=key.length;
			dataFileCounter.valueSpace +=(value == null ? 0 : value.length);
			
			if(value == null || value.length == 0){
				dataFileCounter.remove++;
			}
			else{
				
			}
		}

		return new StorageVaue(
				VCode.getBytes(dataFileId),
				VCode.getBytes(valuePos),
				VCode.getBytes(value == null ? 0 : value.length)
			);
	}

	
	/* (non-Javadoc)
	 * @see com.bitcask.hdb.datafile.IDataFile#flush(boolean)
	 */
	public void flush(boolean force) {
		// TODO Auto-generated method stub
		if(atomicBoolean.get() == true)
			return;
		
		if(!force && this.estimateFileSize - this.fileSize < 1024)
			return;
		
		atomicBoolean.set(true);
		if(output != null){
			try {
				this.fileDescriptor.sync();
				fileSize=estimateFileSize;
			} catch (SyncFailedException e) {
				// TODO Auto-generated catch block
			}
		}
		atomicBoolean.set(false);
	}
	
	public void close() throws IOException{
		if(output != null){
			timer.cancel();
			timer=null;
			fileDescriptor.sync();
			output.close();
			fileDescriptor=null;
			output = null;
		}
	}
	
	@Override
	protected void finalize(){
		if(output != null){
			try {
				this.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
		}
	}
	
	/**
	 * 定时刷新磁盘
	 */
	private class ForceFlush extends TimerTask{

		public void run() {
			DataFile.this.flush(false);
		}

	}
	
}
