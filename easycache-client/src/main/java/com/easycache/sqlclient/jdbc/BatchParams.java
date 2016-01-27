/*
 * jiang yong 2014.12-2015.01
 */
package com.easycache.sqlclient.jdbc;

import java.util.ArrayList;
import java.util.HashMap;

import com.easycache.sqlclient.type.SqlKind;
import com.hazelcast.easycache.utility.IMDGString;

public class BatchParams {
	
	private SqlKind sqlkind;
	private String sqlsentence;
	private ArrayList<String> arguNameList = new ArrayList<String>();
	private int batchNum = 0;
	private HashMap<String, ArrayList<String>> arguValueListMap = new HashMap<String, ArrayList<String>>();
	
	public BatchParams(SqlKind sqlkind, String sqlsentence, ArrayList<String> arguNameList){
			this.setSqlKind(sqlkind);
			this.setSqlSentence(sqlsentence);
			this.setArguNameList(arguNameList);
	}
	
	private void setSqlKind(SqlKind sqlkind){
		this.sqlkind = sqlkind;
	}
	
	private void setSqlSentence(String sqlsentence){
		this.sqlsentence = new String(sqlsentence);
	}
	
	private void setArguNameList(ArrayList<String> arguNameList){
		for(int i = 0; i < arguNameList.size(); i++){
			this.arguNameList.add(arguNameList.get(i));
		}
	}
	
	public SqlKind getSqlKind(){
		return this.sqlkind;
	}
	
	public String getSqlSentence(){
		return sqlsentence;
	}
	
	public ArrayList<String> getArguNameList(){
		return this.arguNameList;
	}
 
	public HashMap<String, ArrayList<String>>  getArguValueListMap(){
		return this.arguValueListMap;
	}
	
	public void addArguValueList(ArrayList<String> arguValueList){
		batchNum++;
		ArrayList<String> arguValueListTemp = new ArrayList<String>();
		for(int i = 0; i < arguValueList.size(); i++){
			arguValueListTemp.add(arguValueList.get(i));
		}
		arguValueListMap.put(IMDGString.BATCH_KEY + batchNum, arguValueListTemp);
		
	}
	
	public void clear(){
		batchNum = 0;
		arguValueListMap.clear();
	}
}
