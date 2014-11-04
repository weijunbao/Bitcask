package com.bitcask.hdb.common;

import junit.framework.TestCase;

import org.junit.Test;

public class testVCode  extends TestCase{
	@Test
	public void test(){
		long timestamp=1414832685806L;
		byte [] buffer=VCode.getBytes(timestamp);
		long timestamp_=VCode.getVCode(buffer, 0, null);
		System.out.println(timestamp);
		System.out.println(timestamp_);
		assertEquals(timestamp - timestamp_,0);
	}
}
