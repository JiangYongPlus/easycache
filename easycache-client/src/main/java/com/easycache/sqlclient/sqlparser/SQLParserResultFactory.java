package com.easycache.sqlclient.sqlparser;

import java.util.HashMap;
import java.util.Map;

public class SQLParserResultFactory {
	private static Map<String, InsertParserResult> insertParserResultMap= new HashMap<String, InsertParserResult>();
	private static Map<String, DeleteParserResult> deleteParserResultMap= new HashMap<String, DeleteParserResult>();
	private static Map<String, UpdateParserResult> updateParserResultMap= new HashMap<String, UpdateParserResult>();
	private static Map<String, SelectParserResult> selectParserResultMap= new HashMap<String, SelectParserResult>();

	public static InsertParserResult getInsertParserResult(String key){
		return insertParserResultMap.get(key);
	}
	public static DeleteParserResult getDeleteParserResult(String key){
		return deleteParserResultMap.get(key);
	}	
	public static UpdateParserResult getUpdateParserResult(String key){
		return updateParserResultMap.get(key);
	}
	public static SelectParserResult getSelectParserResult(String key){
		return selectParserResultMap.get(key);
	}

	public static void setInsertParserResult(String key, InsertParserResult insertParserResult){
		insertParserResultMap.put(key, insertParserResult);
	}
	public static void setDeleteParserResult(String key, DeleteParserResult deleteParserResult){
		deleteParserResultMap.put(key, deleteParserResult);
	}	
	public static void setUpdateParserResult(String key, UpdateParserResult updateParserResult){
		updateParserResultMap.put(key, updateParserResult);
	}
	public static void setSelectParserResult(String key, SelectParserResult selectParserResult){
		selectParserResultMap.put(key, selectParserResult);
	}
}
