package com.bitcask.hdb.common;

/**
 * @author weijunbao
 * */
public final class StringHelper {
	public static boolean isNullOrEmpty(String str){
		return str == null || str.equals("") ? true : false;
	}
}
