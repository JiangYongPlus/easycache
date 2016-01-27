package com.easycache.sqlclient;

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.easycache.serialization.HazelcastObject;

public class TableMetaData extends HazelcastObject {
	private String tableName;
	private ArrayList<String> columnNameList = new ArrayList<String>();
	
	@SuppressWarnings("rawtypes")
	private ArrayList<Class> columnClassList = new ArrayList<Class>();
	private ArrayList<String> primaryKeyList = new ArrayList<String>();
	
	
	public ArrayList<String> getColumnNameList() {
		return columnNameList;
	}
	@SuppressWarnings("rawtypes")
	public ArrayList<Class> getColumnClassList() {
		return columnClassList;
	}

	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void addColumnName(String columnName){
		columnNameList.add(columnName);
	}
	public String getColumnName(int index){
		return columnNameList.get(index);
	}
	
	public int getColumnSize(){
		return columnNameList.size();
	}
	
	@SuppressWarnings("rawtypes")
	public void addColumnClass(Class columnClass){
		columnClassList.add(columnClass);
	}
	
	public void addPrimaryKey(String primaryKey){
		primaryKeyList.add(primaryKey);
	}
	
	public List<String> getPrimaryKeyList(){
		return primaryKeyList;
	}
	
	public int getPrimaryKeyListSize(){
		return primaryKeyList.size();
	}
	
	public boolean judgeTableAttribute(String attributeName){
		for(String columnName : columnNameList){
			if(columnName.equalsIgnoreCase(attributeName)){
				return true;
			}
		}
		return false;
	}
	
	@SuppressWarnings("rawtypes")
	public Class getColumnClassByAttributeName(String columnName){
		for(int i = 0; i < columnNameList.size(); i++){
			if(columnNameList.get(i).equals(columnName)){
				return columnClassList.get(i);
			}
		}
		return String.class;
	}
	@SuppressWarnings("rawtypes")
	public Class getColumnClassByAttributeIndex(int index) throws Exception {
		if(columnClassList.size() <= index) {
			throw new Exception("columnClassList out of bound:" + index);
		}
		return columnClassList.get(index);
	}
}
