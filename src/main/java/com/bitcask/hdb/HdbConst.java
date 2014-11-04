package com.bitcask.hdb;


/**
 * @author weijunbao
 *
 */
public final class HdbConst {
	/*
	 * 文件名后缀
	 * */
	public static final String DATA_FILE_EXTENSION =".datafile";
	
	/*
	 * 文件名前缀
	 * */
	public static final String DATA_FILE_PREFIX="";
	
	/*
	 * Hint文件,用于快速系统快速启动
	 * */
	public static final String HINT_FILE_EXTENSION=".hintfile";
	
	/*
	 * 系统 OK 文件,代表系统正常结束。
	 * 正常结束的话,所有的数据都可以从hint文件中快速恢复
	 * */
	public static final String OK_FILE="hdb.ok";
	
	/*
	 * Merge Ok文件
	 * */
	public static final String MERGE_OK_FILE="hdb.merge.ok";
	/*
	 * merge文件。暂时没用
	 * */
	public static final String MERGE_FILE_EXTENSION=".merge";
	
	/*
	 * 系统支持的最大key大小,单位字节
	 * */
	public static final int MAX_KEY_SIZE=32;
	
	/*
	 * 系统支持的最大value大小,单位字节
	 * */
	public static final int MAX_VALUE_SIZE=1024 * 1024;
	
	/*
	 * 当系统 数据文件数量大于 MERGE_FILE_SIZE时,才考虑进行Merge工作
	 * */
	public static final int MERGE_FILE_SIZE=10;
	
	/*
	 * 强制Merge文件数,但数据文件大于FORCE_MERGE_FILE_SIZE时,强制Merge
	 * */
	public static final int FORCE_MERGE_FILE_SIZE=100;
	
	/*
	 * Merge cache
	 * */
	public static final int MERGE_CACHE_SIZE=1000;
}
