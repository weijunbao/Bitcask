package com.bitcask.hdb.datafile;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.bitcask.hdb.common.BitConverter;

public final class HintFile {
	private OutputStream output;
	private long hintFileId;
	
	public HintFile(long hintFileId) throws FileNotFoundException{
		this.hintFileId=hintFileId;
		output=new FileOutputStream(DataFileHelper.getHintFilePath(hintFileId));
	}
	
	public void write(long timestamp,int keySize,int valueSize,int valuePos,byte key[]) throws IOException{
		if(output != null){
			output.write(BitConverter.getBytes(timestamp));
			output.write(BitConverter.getBytes(keySize));
			output.write(BitConverter.getBytes(valueSize));
			output.write(BitConverter.getBytes(valuePos));
			output.write(key);
		}
	}
	
	public void close() throws IOException{
		if(output !=null){
			output.close();
			output=null;
		}
	}
	
	@Override
	public void finalize(){
		try {
			close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}
}
