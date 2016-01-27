package com.easycache.sqlclient.sqlparser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectParserResult {
	boolean isSupportedSql;
	Map<String, String> tableNameAliasMap;
	List<Item> itemList;
	List<Predicate> predicateList;
	
	public void setIsSupportedSql(boolean isSupportedSql) {
		this.isSupportedSql = isSupportedSql;
	}
	
	public void setTableNameAliasMap(Map<String, String> tableNameAliasMap) {
		this.tableNameAliasMap = tableNameAliasMap;
	}
	
	public void setItemList(List<Item> itemList) {
		this.itemList = itemList;
	}
	
	public void setPredicateList(List<Predicate> predicateList) {
		this.predicateList = predicateList;
	}
	
	public boolean getIsSupportedSql() {
		return isSupportedSql;
	}
	public Map<String, String> getTableNameAliasMap() {
		return tableNameAliasMap;
	}
	
	public List<Item> getItemList() {
		return itemList;
	}
	
	public List<Predicate> duplicatePredicateList(String selectSentence) throws SQLException {
		if(predicateList == null){
			return null;
		}
		List<Predicate> tempPredicateList = new ArrayList<Predicate>();
		for(int i = 0; i < predicateList.size(); i++){
			Predicate tempPredicate = predicateList.get(i).duplicate();
			tempPredicateList.add(tempPredicate);
		}
		for(int i = 0; i < tempPredicateList.size(); i++){
			if(tempPredicateList.get(i) instanceof QueryPredicate){
				((QueryPredicate)tempPredicateList.get(i)).setAttributeValues(selectSentence);
			}
			tempPredicateList.get(i).setActive();
		}
		return tempPredicateList;
	}
	
	public List<Predicate> duplicatePredicateList(List<String> attributeNameList, List<String> attributeValueList) throws SQLException {
		if(predicateList == null){
			return null;
		}
		List<Predicate> tempPredicateList = new ArrayList<Predicate>();
		for(int i = 0; i < predicateList.size(); i++){
			Predicate tempPredicate = predicateList.get(i).duplicate();
			tempPredicateList.add(tempPredicate);
		}
		for(int i = 0; i < tempPredicateList.size(); i++){
			if(tempPredicateList.get(i) instanceof QueryPredicate){
				((QueryPredicate)tempPredicateList.get(i)).setAttributeValues(attributeNameList, attributeValueList);
			}
			tempPredicateList.get(i).setActive();
		}
		return tempPredicateList;
	}	
}
