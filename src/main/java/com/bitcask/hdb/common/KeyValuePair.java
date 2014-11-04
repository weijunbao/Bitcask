package com.bitcask.hdb.common;

import java.io.Serializable;

public class KeyValuePair<K, V> implements Serializable
{
	private static final long serialVersionUID = -7308950510798096639L;
	
	private K key;
	private V value;
	
	public KeyValuePair(){}
	
	public KeyValuePair(K key, V value)
	{
		this.key = key;
		this.value = value;
	}
	
	public K getKey()
	{
		return key;
	}

	public V getValue()
	{
		return value;
	}

	public void setKey(K key)
	{
		this.key = key;
	}

	public void setValue(V value)
	{
		this.value = value;
	}
}
