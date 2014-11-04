package com.bitcask.hdb.common;

public class Outter<E> {
	private E value;

	public Outter() {
	}

	public Outter(E value) {
		this.value = value;
	}

	public E value() {
		return this.value;
	}

	public void setValue(E value) {
		this.value = value;
	}
}
