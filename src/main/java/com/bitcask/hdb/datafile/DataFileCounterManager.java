package com.bitcask.hdb.datafile;

import java.util.HashMap;
import java.util.Map;

/**
 * @author weijunbao
 * */

public final class DataFileCounterManager {
	private static Map<Long,DataFileCounter> m=new HashMap<Long,DataFileCounter>();
	
	public static synchronized DataFileCounter get(int dataFileId){
		return m.get(dataFileId);
	}
	
	public static synchronized void put(long dataFileId,DataFileCounter dataFileCounter){
		m.put(dataFileId, dataFileCounter);
	}
	
	public static synchronized void remove(int dataFileId){
		m.remove(dataFileId);
	}
}
