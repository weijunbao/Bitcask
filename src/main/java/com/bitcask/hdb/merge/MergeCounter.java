package com.bitcask.hdb.merge;

import java.io.Serializable;
import java.util.List;

/**
 * @author weijunbao
 * Merge计数器
 * */
public final class MergeCounter implements Serializable {
	private static final long serialVersionUID = 3450206203645167655L;

	/*Merge 开始 时间*/
	private long startTime;
	
	/*
	 * Merge 结束时间
	 * */
	private long endTime;
	
	/*
	 * Merge的文件数
	 * */
	private List<Integer> filesBeforMerge;
	
	/*
	 * Merge后的文件数
	 * */
	private List<Integer> filesAfterMerge;
	
	/*
	 * Merge前的文件总大小
	 * */
	private long fileLengthBeforMerge;
	
	/*
	 * Merge后的文件总大小
	 * */
	private long fileLengthAfterMerge;
	
	/*
	 * Merge的过程中是否有错误
	 * */
	private boolean error;
	
	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public List<Integer> getFilesBeforMerge() {
		return filesBeforMerge;
	}

	public void setFilesBeforMerge(List<Integer> filesBeforMerge) {
		this.filesBeforMerge = filesBeforMerge;
	}

	public List<Integer> getFilesAfterMerge() {
		return filesAfterMerge;
	}

	public void setFilesAfterMerge(List<Integer> filesAfterMerge) {
		this.filesAfterMerge = filesAfterMerge;
	}

	public long getFileLengthBeforMerge() {
		return fileLengthBeforMerge;
	}

	public void setFileLengthBeforMerge(long fileLengthBeforMerge) {
		this.fileLengthBeforMerge = fileLengthBeforMerge;
	}

	public long getFileLengthAfterMerge() {
		return fileLengthAfterMerge;
	}

	public void setFileLengthAfterMerge(long fileLengthAfterMerge) {
		this.fileLengthAfterMerge = fileLengthAfterMerge;
	}

	public boolean hasError(){
		return this.error;
	}
	
	public void setHasError(boolean hasError){
		this.error=hasError;
	}
	
	@Override
	public String toString(){
		StringBuilder strBuilder=new StringBuilder();
		strBuilder.append("StartTime=" +this.startTime);
		return strBuilder.toString();
	}
}
