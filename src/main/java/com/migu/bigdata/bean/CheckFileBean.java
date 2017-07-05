package com.migu.bigdata.bean;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

import com.migu.bigdata.mapreduce.EtlJob;

/**
 * 描述check文件记录结构
 * @author FengL
 *
 */
public class CheckFileBean {
	private static Logger log = Logger.getLogger(CheckFileBean.class);
	String fileName;
	Long   fileSize=0L;
	Long   recordRows=0L;
	String dataTime;
	String accessTime;
	
	public CheckFileBean(String line){
		String cols[]=line.split("€");
		this.fileName=cols[0];
		this.fileSize=Long.parseLong(cols[1]);
		this.recordRows=Long.parseLong(cols[2]);
		this.dataTime=cols[3];
		this.accessTime=cols[4];
	}
	
	public String printRows(){
		String rows="";
		rows=String.format("%s %d %d %s %s",this.fileName,this.fileSize,this.recordRows,this.dataTime,this.accessTime);
		return rows;
	}
	public String getFileName() {
		return fileName;
	}
	
	
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public Long getFileSize() {
		return fileSize;
	}
	public void setFileSize(Long fileSize) {
		this.fileSize = fileSize;
	}
	public Long getRecordRows() {
		return recordRows;
	}
	public void setRecordRows(Long recordRows) {
		this.recordRows = recordRows;
	}
	public String getDataTime() {
		return dataTime;
	}
	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}
	public String getAccessTime() {
		return accessTime;
	}
	public void setAccessTime(String accessTime) {
		this.accessTime = accessTime;
	}
	
	
}


