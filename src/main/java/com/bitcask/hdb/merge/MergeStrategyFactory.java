package com.bitcask.hdb.merge;

import com.bitcask.hdb.Hdb;
import com.bitcask.hdb.config.HdbConfig;

/**
 * @author weijunbao
 * */
public final class MergeStrategyFactory {
	private MergeStrategyFactory(){}
	
	public static MergeStrategy createMergeStrategy(Hdb hdb){
		MergeType mergeType=MergeType.fromString(HdbConfig.getCurrent().getProperty("merge_type"));
		if(mergeType.equals(MergeType.Fixed)){
			return new FixedMergeStrategy(hdb);
		}
		else if(mergeType.equals(MergeType.Timer)){
			return new TimerMergeStrategy(hdb);
		}
		return null;
	}
}
