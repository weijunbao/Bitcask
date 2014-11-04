package com.bitcask.hdb.merge;

import java.util.Timer;
import java.util.TimerTask;

import com.bitcask.hdb.Hdb;
import com.bitcask.hdb.config.HdbConfig;

/**
 * @author weijunbao
 * */
public final class TimerMergeStrategy extends MergeStrategy{
	Timer timer=new Timer();
	
	public TimerMergeStrategy(Hdb<?,?> hdb) {
		super(hdb);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void start(){
		timer.schedule(new MergeTimer(), 0,Long.parseLong(HdbConfig.getCurrent().getProperty("merge_delay")));
	}
	
	@Override
	public void stop(){
		
	}

	public class MergeTimer extends TimerTask{
		@Override
		public void run() {
			// TODO Auto-generated method stub
			TimerMergeStrategy.this.merge(true);
		}
	}
}
