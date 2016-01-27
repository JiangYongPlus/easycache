package com.easycache.sqlclient;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.easycache.utility.IMDGString;

public class HazelcastDatabaseMetaData {
	private static HashMap<String, TableMetaData> tableMetaDataMap = new HashMap<String, TableMetaData>();
//	private HazelcastInstance hazelcast = null;
//	private  IMap<String, TableMetaData> tableMetaDataMap = null;
	
//	public HazelcastDatabaseMetaData() {
//		if ((hazelcast = Hazelcast.getHazelcastInstanceByName("IMDG")) == null) {
//			Config config = new XmlConfigBuilder().build();
//			config.setInstanceName("IMDG");
//			hazelcast = Hazelcast.newHazelcastInstance(config);
//		}
//		tableMetaDataMap = hazelcast.getMap(IMDGString.META_DATA);
//	}
//	
	
	public TableMetaData getTableMetaData(String tableName){
		return tableMetaDataMap.get(tableName);
	}
	
	public void setTableMetaData(String tableName, TableMetaData tableMetaData){
		tableMetaDataMap.put(tableName, tableMetaData);
	}
	
	public Set<String> getTableNames(){
		return tableMetaDataMap.keySet();
	}
	
	//jiang yong 2014-12-8
	//this function is never used
	public boolean judgeTableAttribute(String tableAttribute){
		int index = tableAttribute.indexOf(".");
		if(index == -1){
			for(String tableName : tableMetaDataMap.keySet()){
				if(tableMetaDataMap.get(tableName).judgeTableAttribute(tableAttribute)){
					return true;
				}
			}
		}
		else{
			String tableName = tableAttribute.substring(0,index-1);
			String attributeName = tableAttribute.substring(index+1);
			return tableMetaDataMap.get(tableName).judgeTableAttribute(attributeName);
		}
		return false;
	}
	
	//jiang yong 2014-12-25
	//if two tables hava the same column name, the function getTableAttribute_backup()
	// may get error.
	//join query have multiple tables, so we change tableName to tableNameList
	public String getTableAttribute(List<String> tableNameList, String str){
		String tableAttr = null;
		int tag = 0;
		int index = str.indexOf(".");
		if(index == -1){
			for(String tableName : tableNameList){
				if (tableMetaDataMap.get(tableName).judgeTableAttribute(str)) {
					tableAttr = tableName + "." + str.toLowerCase();
					tag++;
				}
			}
			if(tag > 1){
				try {
					tableAttr = null;
					throw new Exception("unknown columnName: " + str);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		else{
			String tableNameFromStr = str.substring(0, index);
			tableAttr = tableMetaDataMap.get(tableNameFromStr) == null ? null : str;
		}
		return tableAttr;
	}
	
	//jiang yong 2014-12-8
	//if two tables hava the same column name, the function getTableAttribute_backup()
	// may get error.
	public String getTableAttribute_backup(String tableName, String str){
		int index = str.indexOf(".");
		if(index == -1){
			if (tableMetaDataMap.get(tableName).judgeTableAttribute(str)) {
				return tableName + "." + str.toLowerCase();
			}
			return null;
		}
		else{
			String tableNameFromStr = str.substring(0, index);
			return tableMetaDataMap.get(tableNameFromStr) == null ? null : str;
		}
	}
	
	public String getTableAttribute_backup(String str){
		int index = str.indexOf(".");
		if(index == -1){
			for(String tableName : tableMetaDataMap.keySet()){
				if(tableMetaDataMap.get(tableName).judgeTableAttribute(str)){
					return tableName+"."+str.toLowerCase();
				}
			}
			return null;
		}
		else{
			String tableName = str.substring(0, index);
			return tableMetaDataMap.get(tableName) == null ? null : str;
		}
	}
	//jiang yong done
}
