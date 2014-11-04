package com.bitcask.hdb;

/**
 * @author weijunbao
 * */

public interface IByte<E> {
	byte[] toByte(E e);
	E get(byte[] body);
	E get(byte[] body,int off,int len);
}
