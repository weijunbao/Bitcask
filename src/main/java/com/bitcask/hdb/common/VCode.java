package com.bitcask.hdb.common;


/**
 * variable length coding
 * 
 * @author weijunbao
 *
 */
public final class VCode {
	private final static long VCODE1=127L;
	private final static long VCODE2 =16383L;
	private final static long VCODE3=2097151L;
	private final static long VCODE4=268435455L;
	private final static long VCODE5=34359738367L;
	private final static long VCODE6=4398046511103L;
	private final static long VCODE7=562949953421311L;
	private final static long VCODE8=72057594037927935L;
	private final static long VCODE9=9223372036854775807L;
	
	/**
	 * @param v
	 * @return	
	 */
	public static int getVSize(long v){
		int bytes=1;
		if(v < 0)
			throw new IllegalArgumentException("v less than zero");
		
		if(v <= VCODE1)		 bytes=1;
		else if(v<=VCODE2) bytes=2;
		else if(v<=VCODE3) bytes=3;
		else if(v<=VCODE4) bytes=4;
		else if(v<=VCODE5) bytes=5;
		else if(v<=VCODE6) bytes=6;
		else if(v<=VCODE7) bytes=7;
		else if(v<=VCODE8) bytes=8;
		else if(v<=VCODE9) bytes=9;
		else bytes=10;
		return bytes;
	}
	
	public static byte[] getBytes(byte v){
		assert v > 0;
		
		byte [] dataBuffer=new byte[getVSize(v)];
		setVCode(v,dataBuffer,0);
		return dataBuffer;
	}
	
	public static byte[] getBytes(short v){
		assert v > 0;
		
		byte [] dataBuffer=new byte[getVSize(v)];
		setVCode(v,dataBuffer,0);
		return dataBuffer;
	}
	
	public static byte[] getBytes(int v){
		assert v > 0;
		byte [] dataBuffer=new byte[getVSize(v)];
		setVCode(v,dataBuffer,0);
		return dataBuffer;
	}
	
	public static byte[] getBytes(long v){
		assert v > 0;
		byte [] dataBuffer=new byte[getVSize(v)];
		setVCode(v,dataBuffer,0);
		return dataBuffer;
	}
	
	public static int setVCode(byte v,final byte[] dataBuffer,int offset){
		if(v < 0)
			throw new IllegalArgumentException("v less than zero");
		
		int len=getVSize(v);
		int bytes=0,i;
		while(v > 0){
			byte c = (byte)(v & 0x7F);
			dataBuffer[offset + (--len)] = c;
			bytes+=1;
			v >>>= 7;
		}

		offset +=bytes;
		for(i=0;i<bytes-1; i++){
			dataBuffer[offset - bytes + i] |=(0x01<<7);
		}
		assert(bytes > 0);
		return bytes;
	}

	public static int setVCode(short v,final byte[] dataBuffer,int offset){
		if(v < 0)
			throw new IllegalArgumentException("v less than zero");
		
		int len=getVSize(v);
		int bytes=0,i;
		while(v > 0){
			byte c = (byte)(v & 0x7F);
			dataBuffer[offset + (--len)] = c;
			bytes+=1;
			v >>>= 7;
		}

		offset +=bytes;
		for(i=0;i<bytes-1; i++){
			dataBuffer[offset - bytes + i] |=(0x01<<7);
		}
		assert(bytes > 0);
		return bytes;
	}
	
	public static int setVCode(int v,final byte[] dataBuffer,int offset){
		if(v < 0)
			throw new IllegalArgumentException("v less than zero");
		
		int len=getVSize(v);
		int bytes=0,i;
		while(v > 0){
			byte c = (byte)(v & 0x7F);
			dataBuffer[offset + (--len)] = c;
			bytes+=1;
			v >>>= 7;
		}

		offset +=bytes;
		for(i=0;i<bytes-1; i++){
			dataBuffer[offset - bytes + i] |=(0x01<<7);
		}
		assert(bytes > 0);
		return bytes;
	}
	
	/**
	 * @param v
	 * @param dataBuffer
	 * @param offset
	 * @return
	 */
	public static int setVCode(long v,final byte[] dataBuffer,int offset){
		if(v < 0)
			throw new IllegalArgumentException("v less than zero");
		
		int len=getVSize(v);
		int bytes=0,i;
		while(v > 0){
			byte c = (byte)(v & 0x7F);
			dataBuffer[offset + (--len)] = c;
			bytes+=1;
			v >>>= 7;
		}

		offset +=bytes;
		for(i=0;i<bytes-1; i++){
			dataBuffer[offset - bytes + i] |=(0x01<<7);
		}
		assert(bytes > 0);
		return bytes;
	}
	
	/**
	 * @param dataBuffer变长编码字节数组
	 * @param offset	变长编码开始索引
	 * @param outSize	变长编码占用字节数
	 * @return 			变长编码值
	 */
	public static long getVCode(final byte[] dataBuffer,int offset,Outter<Integer> outSize){
		int vsize;
		long vcode=0;
		vsize = 0;
		assert(dataBuffer !=null);
		
		do{
			vcode |= (byte) (dataBuffer[offset] & 0x7f);
			vcode <<= 7;
			vsize += 1;
		}
		while((byte)(dataBuffer[offset++] & 0x80) != 0);
		vcode >>=7;
		
		if(outSize != null)
			outSize.setValue(vsize);
		
		return vcode;
	}
}
