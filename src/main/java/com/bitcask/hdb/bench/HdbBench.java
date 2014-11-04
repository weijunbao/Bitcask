package com.bitcask.hdb.bench;

import java.util.Random;

import com.bitcask.hdb.Hdb;
import com.bitcask.hdb.IByte;
import com.bitcask.hdb.common.BitConverter;

public class HdbBench {
	public static class IntegerByteConverter implements IByte<Integer>{

		public byte[] toByte(Integer i) {
			// TODO Auto-generated method stub
			return BitConverter.getBytes(i);
		}

		public Integer get(byte[] body) {
			// TODO Auto-generated method stub
			return BitConverter.getInt(body,0);
		}

		public Integer get(byte[] body, int off, int len) {
			// TODO Auto-generated method stub
			return BitConverter.getInt(body, off);
		}
		
	}
	
	public static class StringByteConverter implements IByte<String>{

		public byte[] toByte(String string) {
			// TODO Auto-generated method stub
			return string.getBytes();
		}

		public String get(byte[] body) {
			// TODO Auto-generated method stub
			return new String(body);
		}

		public String get(byte[] body, int off, int len) {
			// TODO Auto-generated method stub
			return new String(body,off,len);
		}
	}
	
	private static String makeString(){
		StringBuilder strBuilder=new StringBuilder();
		String template="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
		Random random=new Random();
		int randomNumber;
		
		while( (randomNumber=Math.abs(random.nextInt()) % 3000) ==0){}
		for(int i=0;i<randomNumber;i++){
			int index = Math.abs(random.nextInt()) % template.length();
			if(index < 0){
				i--;
				continue;
			}
			strBuilder.append(template.charAt(index));
		}
		return strBuilder.toString();
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		long startTime,endTime;
		Hdb<Integer,String> hdb=new Hdb<Integer, String>(new IntegerByteConverter(),new StringByteConverter());
		hdb.open();
		int B=1000000;
		startTime=System.currentTimeMillis();
		for(int i=0;i<B;i++){
			hdb.put(i, makeString());
		}
		endTime=System.currentTimeMillis();
		System.out.println("插入="+(endTime -startTime) / 1000);
		
		
		startTime=System.currentTimeMillis();
		for(int i=0;i<B;i++){
			hdb.remove(i);
		}
		endTime=System.currentTimeMillis();
		System.out.println("删除="+(endTime -startTime) / 1000);
		System.out.println("OK");
		
	}

}
