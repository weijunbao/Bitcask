package com.bitcask.hdb.datafile;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.bitcask.hdb.common.BitConverter;

public final class HintFileReader {
	private InputStream input;
	public HintFileReader(String name) throws FileNotFoundException{
		input=new FileInputStream(name);
	}
	
	public HintFileRecord readNext(){
		if(input == null)
			return null;
		
		try{
			byte intBuffer[]=new byte[Integer.SIZE / 8];
			byte longBuffer[]=new byte[Long.SIZE / 8];
			int timestamp;
			int keySize = 0,valueSize,valuePos;
			byte key[];
			
			if(input.read(longBuffer) != -1){
				throw new Exception();
			}
			timestamp=BitConverter.getInt(longBuffer,0);
			
			if(input.read(intBuffer) == -1){
				throw new Exception();
			}
			keySize=BitConverter.getInt(intBuffer,0);
			
			if(input.read(intBuffer) == -1){
				throw new Exception();
			}
			valueSize=BitConverter.getInt(intBuffer,0);
			
			if(input.read(intBuffer) == -1){
				throw new Exception();
			}
			valuePos=BitConverter.getInt(intBuffer,0);
			
			key=new byte[keySize];
			if(input.read(key) == -1){
				throw new Exception();
			}
			return new HintFileRecord(timestamp,keySize,valueSize,valuePos,key);
		}
		catch(IOException ex){
		}
		catch(Exception ex){
		}
		return null;
	}
	
	@Override
	protected void finalize(){
		if(input !=null){
			try {
				input.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			input=null;
		}
	}
}
