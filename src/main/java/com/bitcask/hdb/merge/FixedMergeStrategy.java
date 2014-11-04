package com.bitcask.hdb.merge;

import com.bitcask.hdb.Hdb;
import com.bitcask.hdb.common.Delegate1;

/*
 * 固定文件数
 * */
public class FixedMergeStrategy extends MergeStrategy {
	
	public FixedMergeStrategy(Hdb<?,?> hdb) {
		super(hdb);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void start(){
		hdb.getDataFileManager().setMergeFunc(new Delegate1<Object>(){
			public void run(Object e) {
				// TODO Auto-generated method stub
				new Thread(new Runnable(){
					public void run() {
						// TODO Auto-generated method stub
						FixedMergeStrategy.this.merge(false);
					}}).start();
			}});
	}
	
	@Override
	public void stop(){
		
	}
}
