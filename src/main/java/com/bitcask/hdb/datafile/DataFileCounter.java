package com.bitcask.hdb.datafile;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author weijunbao
 * 
 * */
public final class DataFileCounter implements Serializable{
	private static final long serialVersionUID = -6934365530327577258L;
	
	//总共记录数
	public int total;
	//删除的记录数
	public int remove;
	public int keySpace;
	public int valueSpace;
	public int removeKeySpace;
	public int removeValueSpace;
	//文件总大小
	public int fileSize;
	
	//空间利用率
	public int SpaceRate(){
		return 1;
	}

	public int getTotal() {
		return total;
	}

	public int getRemove() {
		return remove;
	}

	public int getKeySpace() {
		return keySpace;
	}

	public int getValueSpace() {
		return valueSpace;
	}

	public int getRemoveKeySpace() {
		return removeKeySpace;
	}

	public int getRemoveValueSpace() {
		return removeValueSpace;
	}
}
