package com.easycache.sqlclient.sqlparser;


public class Item {
	private String tableName;
	private String attributeName;
	
	public Item(String tableName, String attributeName){
		this.tableName = tableName;
		this.attributeName = attributeName;
	}
	public String getTableName() {
		return tableName;
	}
	public String getAttributeName() {
		return attributeName;
	}
	public String toString(){
		return tableName + "." + attributeName;
	}
	public Item(String tableAttribute){
		int index = tableAttribute.indexOf(".");
		tableName = tableAttribute.substring(0, index);
		attributeName = tableAttribute.substring(index+1);
	}
}
