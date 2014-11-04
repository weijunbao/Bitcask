package com.bitcask.hdb.common;

/**
 * @author weijunbao
 *
 * 
 */
public class Scope<T extends Number & Comparable<T>> {
	private T start;
	private T end;
	
	public Scope(T start,T end){
		this.start=start;
		this.end=end;
	}
	
	public T getStart(){
		return this.start;
	}
	
	public T getEnd(){
		return this.end;
	}
	
}
