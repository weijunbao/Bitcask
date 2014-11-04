package com.bitcask.hdb.datafile;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bitcask.hdb.common.BitConverter;

public final class DataFileReader {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFileReader.class);
	private InputStream input;
	private CRC32 crc32=new CRC32();
	private int filesize=0;
	
	public DataFileReader(String name) throws FileNotFoundException{
		input=new FileInputStream(name);
	}
	
	public DataFileRecord readNext(){
		if(input==null)
			return null;
		byte intBuffer[]=new byte[Integer.SIZE / 8];
		byte longBuffer[]=new byte[Long.SIZE / 8];
		byte key[],value[] = null;
		int keySize,valueSize;
		long timestamp,crc;
		try{
			int valuePos = 0;
			//read crc
			if(input.read(longBuffer) == -1)
				throw new Exception();
			filesize +=8;
			crc=BitConverter.getLong(longBuffer,0);
			
			//read timestamp
			if(input.read(longBuffer) == -1)
				throw new Exception();
			filesize +=8;
			timestamp=BitConverter.getLong(longBuffer,0);
			
			//read keysize
			if(input.read(intBuffer) == -1)
				throw new Exception();
			filesize +=4;
			keySize=BitConverter.getInt(intBuffer,0);
			
			//read valuesize
			if(input.read(intBuffer) == -1)
				throw new Exception();
			filesize +=4;
			
			valueSize=BitConverter.getInt(intBuffer,0);
			
			key=new byte[keySize];
			if(input.read(key) == -1)
				throw new Exception();
			
			filesize+=keySize;
			valuePos=filesize;
			
			if(valueSize > 0){
				value=new byte[valueSize];
				if(input.read(value) == -1)
					throw new Exception();
				filesize += valueSize;
			}
			return new DataFileRecord(crc,timestamp,keySize,valueSize,key,value,valuePos);
		}
		catch(IOException ex){
			LOGGER.error("",ex);
		}
		catch(Exception ex){
			LOGGER.error("",ex);
		}
		return null;
	}

	public void close() throws IOException{
		if(input != null){
			input.close();
			input=null;
		}
	}
	
	@Override
	protected void finalize(){
		if(input != null){
			try {
				this.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
		}
	}
}
