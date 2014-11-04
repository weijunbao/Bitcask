package com.bitcask.hdb;

public interface IDbOperate<K,V> {
	void open();
	V get(K key);
	void put(K key,V value) throws Exception;
	void remove(K key) throws Exception;
	void recover();
	void close() throws Exception;
}
