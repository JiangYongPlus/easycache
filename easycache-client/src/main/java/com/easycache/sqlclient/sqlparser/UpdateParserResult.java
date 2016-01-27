package com.easycache.sqlclient.sqlparser;

public class UpdateParserResult {
	boolean isSupportedSql;
	private String tableName;
	private QueryPredicate queryPredicate;
	
	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public QueryPredicate getQueryPredicate() {
		return queryPredicate;
	}
	
	public void setQueryPredicate(QueryPredicate queryPredicate) {
		this.queryPredicate = queryPredicate;
	}
	
	public void setIsSupportedSql(boolean isSupportedSql) {
		this.isSupportedSql = isSupportedSql;
	}
	
	public boolean getIsSupportedSql() {
		return isSupportedSql;
	}
}
