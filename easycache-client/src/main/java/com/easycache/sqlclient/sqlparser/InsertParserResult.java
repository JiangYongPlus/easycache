package com.easycache.sqlclient.sqlparser;

public class InsertParserResult {
	boolean isSupportedSql;
	private String tableName;
	
	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void setIsSupportedSql(boolean isSupportedSql) {
		this.isSupportedSql = isSupportedSql;
	}
	
	public boolean getIsSupportedSql() {
		return isSupportedSql;
	}
}
