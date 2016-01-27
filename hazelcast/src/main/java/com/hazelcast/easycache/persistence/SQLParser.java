package com.hazelcast.easycache.persistence;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class SQLParser {

	public static final String SPLIT_LABEL = "\\$#@";
	public static final String PREFIX = "$cglib_prop_";
	public static final int PREFIX_LEN = PREFIX.length();
	
	private String insertSQL = null;
	private String updateSQL = null;
	private String deleteSQL = null;
	private String querySQL = null;
	 
	private ArrayList<String> insertCols;
	private ArrayList<String> updateCols;
	private ArrayList<String> queryCols;
	private ArrayList<String> deleteCols;
	
	private boolean insertInit = false;
	private boolean updateInit = false;
	private boolean deleteInit = false;
	private boolean queryInit = false;
	
	private Object insertLockObject = new Object();
	private Object deleteLockObject = new Object();
	private Object queryLockObject = new Object();
	private Object updateLockObject = new Object();
	
	public SQLParser(){
		insertInit = false;
		updateInit = false;
		deleteInit = false;
		queryInit = false;
		insertCols = new ArrayList<String>(7);
		updateCols = new ArrayList<String>(7);
		queryCols = new ArrayList<String>(2);
		deleteCols = new ArrayList<String>(2);
	}
	
	public String getInsertSQLFromObject(Object object, String tableName){	
		if(insertInit)
			return insertSQL;
		else {
			synchronized (insertLockObject) {
				if(!insertInit){
					insertSQL = "insert into " + tableName + "(";	
					String cols = new String();
					String vals = new String();
					try {
						for(Field field : object.getClass().getDeclaredFields()){
							field.setAccessible(true);
							String name = field.getName();
							if(name.startsWith(PREFIX))
								name = name.substring(PREFIX_LEN);
							vals += "?,";
							cols += name + ",";
							insertCols.add(name);
						}
					} catch (SecurityException e) {
						e.printStackTrace();
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} 
					vals = vals.substring(0, vals.length()-1) + ")";
					cols = cols.substring(0, cols.length()-1) + ")";
					insertSQL += (cols + " values(" + vals);
					insertInit = true;
				}
			}
			return insertSQL;
		}
	}
	
	public  String getDeleteSQLFromObject(String key, String tableName){	
		if(deleteInit)
			return deleteSQL;
		else {
			synchronized (deleteLockObject) {
				if(!deleteInit){
					String [] keys = key.split(SPLIT_LABEL);
					if(keys == null || keys.length == 1){
						deleteSQL = "delete from " + tableName + " where " + key.substring(0, key.indexOf("=")) + "=?";
						deleteCols.add(key.substring(0, key.indexOf("=")));
					} else {
						deleteSQL = "delete from " + tableName + " where " + keys[0].substring(0, keys[0].indexOf("=")) + "=?";
						deleteCols.add(keys[0].substring(0, keys[0].indexOf("=")));
						for(int i = 1; i != keys.length; ++i){
							deleteSQL += " and " + keys[i].substring(0, keys[i].indexOf("=")) + "=?";
							deleteCols.add(keys[i].substring(0, keys[i].indexOf("=")));
						}
					}
					deleteInit = true;
				}
			}
			return deleteSQL;
		}
	}
	
	/**
	 * @param key
	 * @param object
	 * @param tableName
	 * @return
	 */
	public String getQuerySQLFromObject(String key, Object object, String tableName){
		if(queryInit)
			return querySQL;
		else {
			synchronized (queryLockObject) {
				if(!queryInit){
					String [] keys = key.split(SPLIT_LABEL);
					if(keys == null || keys.length == 1){
						querySQL = "select * from " + tableName + " where " + key.substring(0, key.indexOf("=")) + "=?";
//						System.out.println(key +" :" + querySQL);
						queryCols.add(key.substring(0, key.indexOf("=")));
					} else {
						querySQL = "select * from " + tableName + " where " + keys[0].substring(0, keys[0].indexOf("=")) + "=?";
						queryCols.add(keys[0].substring(0, keys[0].indexOf("=")));
						for(int i = 1; i != keys.length; ++i){
							querySQL += " and " + keys[i].substring(0, keys[i].indexOf("=")) + "=?";
							queryCols.add(keys[i].substring(0, keys[i].indexOf("=")));
						}
					}
					queryInit = true;
				}
			}
			return querySQL;
		}
	}
	
	public String getUpdateSQLFromObject(String key, Object object, String tableName){	
		if(updateInit)
			return updateSQL;
		else {
			synchronized (updateLockObject) {
				if(!updateInit){
					String [] keys = key.split(SPLIT_LABEL);
					ArrayList<String> cols = new ArrayList<String>(2);
					String suffix = null;
					if(keys == null || keys.length == 1){
						suffix = key.substring(0, key.indexOf("=")) + "=?";
						cols.add(key.substring(0, key.indexOf("=")));
					} else {
						suffix = keys[0].substring(0, keys[0].indexOf("=")) + "=?";
						cols.add(keys[0].substring(0, keys[0].indexOf("=")));
						for(int i = 1; i != keys.length; ++i){
							suffix += " and " + keys[i].substring(0, keys[i].indexOf("=")) + "=?";
							cols.add(keys[i].substring(0, keys[i].indexOf("=")));
						}
					}
					updateSQL = "update " + tableName + " set ";
					try {
						for(Field field : object.getClass().getDeclaredFields()){
							field.setAccessible(true);
							String name = field.getName();
							if(name.startsWith(PREFIX))
								name = name.substring(PREFIX_LEN);
							if(!cols.contains(name)){
								updateSQL += (name + "=" + "?,");	
								updateCols.add(name);
							}
						}
					} catch (SecurityException e) {
						e.printStackTrace();
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} 
					updateSQL = updateSQL.substring(0, updateSQL.length()-1);
					updateSQL += " where " + suffix;
					updateCols.addAll(cols);
					updateInit = true;
				}
			}
			return updateSQL;
		}
	}
	
	public ArrayList<String> getQueryCols(){
		return queryCols;
	}
	
	public ArrayList<String> getInsertCols(){
		return insertCols;
	}
	
	public ArrayList<String> getDeleteCols(){
		return deleteCols;
	}
	
	public ArrayList<String> getUpdateCols(){
		return updateCols;
	}
}