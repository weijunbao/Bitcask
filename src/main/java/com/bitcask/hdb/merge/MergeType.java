package com.bitcask.hdb.merge;

/**
 * @author weijunbao
 * */
public enum MergeType {
	Timer("Timer"),
	Fixed("Fixed");
	private String text;
	
	MergeType(String text){
		this.text=text;
	}
	
	public static MergeType fromString(String text){
		if(text != null){
			for(MergeType mergeType : MergeType.values()){
				if(text.equalsIgnoreCase(mergeType.text))
					return mergeType;
			}
		}
		return MergeType.Fixed;
	}
}
