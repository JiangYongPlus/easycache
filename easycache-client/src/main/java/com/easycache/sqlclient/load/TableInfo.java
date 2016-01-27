package com.easycache.sqlclient.load;

//jiang yong 2014-10-09
public class TableInfo {
	
	private String tableName;
	private long tableSize;
	private long maxKey;
	
	public TableInfo(String tableName){
		this.tableName = tableName;
	}
	public String getTableName(){
		return this.tableName;
	}
	
	public long getTableSize(){
		return this.tableSize;
	}
	
	public long getMaxKey(){
		return this.maxKey;
	}
	
	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	public void setTableSize(long tableSize){
		this.tableSize = tableSize;
	}
	
	public void setMaxKey(long maxKey){
		this.maxKey = maxKey;
	}
}
