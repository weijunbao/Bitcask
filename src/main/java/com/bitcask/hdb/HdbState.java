package com.bitcask.hdb;

/**
 * @author weijunbao
 * */
public enum HdbState {
	Open(0),
	Closing(1),
	Recover(2),
	Running(3),
	Stoped(4);
	
	int value;
	private HdbState(int value){
		this.value=value;
	}
}
