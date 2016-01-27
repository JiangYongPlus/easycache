package com.easycache.sqlclient.sqlparser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryPredicate extends Predicate{
	private String tableName;
	private List<String> attributeNameList;
	private List<AlgebraicRelationship> algebraicRelationshipList;
	private List<String> attributeValueList;
	private List<LogicalRelationship> logicalRelationshipList;
	
	public QueryPredicate(){
		attributeNameList = new ArrayList<String>();
		algebraicRelationshipList = new ArrayList<AlgebraicRelationship>();
		attributeValueList = new ArrayList<String>();
		logicalRelationshipList = new ArrayList<LogicalRelationship>();
	}
	
	public QueryPredicate(boolean bl){
		//it is useful, don't delete it!
		attributeValueList = new ArrayList<String>();
	}
	
	public QueryPredicate addSubQueryPredicate(String strLogicalRelationship, int priority, String tableAttribute, String relationship, String attributeValue){
		int index = tableAttribute.indexOf(".");
		String generatedTableName = tableAttribute.substring(0, index);
		String generatedAttributeName = tableAttribute.substring(index+1);
		return addSubQueryPredicate(strLogicalRelationship, priority, generatedTableName, generatedAttributeName, relationship, attributeValue);
	}
	
	public QueryPredicate addSubQueryPredicate(String strLogicalRelationship, int priority, String tableName, String attributeName, String relationship, String attributeValue){
		if(subQueryNumber == 0){
			this.priority = priority;
			this.tableName = tableName;
			attributeNameList.add(attributeName);
			algebraicRelationshipList.add(new AlgebraicRelationship(relationship));
			attributeValueList.add(attributeValue);
			subQueryNumber++;
		}
		else{
			if(this.priority != priority){
				return null;
			}
			if(!this.tableName.equals(tableName)){
				return null;
			}
			if(strLogicalRelationship != null){
				this.logicalRelationshipList.add(new LogicalRelationship(strLogicalRelationship));
			}
			attributeNameList.add(attributeName);
			algebraicRelationshipList.add(new AlgebraicRelationship(relationship));
			attributeValueList.add(attributeValue);
			subQueryNumber++;			
		}
		return this;
	}	

	public String getTableName() {
		return tableName;
	}

	public int getPriority() {
		return priority;
	}

	public void setLogicalRelationshipList(List<LogicalRelationship> logicalRelationshipList) {
		this.logicalRelationshipList = logicalRelationshipList;
	}

	public String getRequiredKey(List<String> primaryKeyList){
		String requiredKey = "";
		if(attributeNameList.size() == 1 && primaryKeyList.size() ==1){
			if(attributeNameList.get(0).equals(primaryKeyList.get(0))){
				return primaryKeyList.get(0) + "=" + attributeValueList.get(0);
			}
			else{
				return null;
			}
		}
		if(attributeNameList.size() != primaryKeyList.size()){
			return null;
		}
		for(String primaryKey : primaryKeyList){
			requiredKey += primaryKey + "=";
			for(int i = 0; i < attributeNameList.size(); i++){
				if(attributeNameList.get(i).equals(primaryKey)){
					requiredKey += attributeValueList.get(i)+"$#@";
					break;
				}
				if(i == attributeNameList.size()-1){
					return null;
				}
			}
		}
		return requiredKey;
	}
	
	public ArrayList<String> getRequiredKeyList(List<String> primaryKeyList){		
		if(primaryKeyList.size() !=1){
			return null;
		}
		for(String attributeName:attributeNameList){
			if(!attributeName.equals(primaryKeyList.get(0))){
				return null;
			}
		}
		for(LogicalRelationship logicalRelationship: logicalRelationshipList){
			if(!logicalRelationship.equals(LogicalRelationshipOperation.OR)){
				return null;
			}
		}
		
		ArrayList<String> requiredKeyList = new ArrayList<String>();
		for(int i = 0; i < attributeNameList.size(); i++){
			String requiredKey = attributeNameList.get(i) + "=" + attributeValueList.get(i);
			requiredKeyList.add(requiredKey);
		}
		return requiredKeyList;
	}
	
	public String getPredicate(){
		String predicate = "";
		for(int i = 0; i < subQueryNumber; i++){
			if(i==0){
				predicate += attributeNameList.get(i) + algebraicRelationshipList.get(i).toString() + attributeValueList.get(i);
			}
			else{
				predicate += " " + logicalRelationshipList.get(i-1).toString() + " " + attributeNameList.get(i) + algebraicRelationshipList.get(i).toString() + attributeValueList.get(i);
			}
		}
		return predicate;
	}
	
	public void setAttributeValues(String selectSentence) throws SQLException{
		ArrayList<String> attributeValueArray = new ArrayList<String>();
		int index = selectSentence.toLowerCase().indexOf("where");
		String[] strs = selectSentence.substring(index+1).trim().split("'");
		for (int j = 0; j < strs.length; j++) {
			if(j%2 == 0) {
				String[] tmps = strs[j].trim().split("[ =()]+");
				for (int k = 0; k < tmps.length; k++) {
					attributeValueArray.add(tmps[k].trim());
				}
			} else {
				attributeValueArray.add("'" + strs[j] + "'");
			}
		}
		for(int i = 0; i < attributeNameList.size(); i++){			
			for(int j = 0; j < attributeValueArray.size()-1; j++){
				String attributeValueString = attributeValueArray.get(j);
				index = -1;
				if(!attributeValueString.startsWith("'")) index = attributeValueString.indexOf(".");
				if(index != -1){
					attributeValueString = attributeValueString.substring(index+1);
				}
				if(attributeValueString.equals(attributeNameList.get(i))){
					if(!attributeValueArray.get(j+1).startsWith("'") && attributeValueArray.get(j+1).contains(".")
							|| attributeValueArray.get(j+1).equals("or") || attributeValueArray.get(j+1).equals("and")){
						continue;
					}
					else{
						attributeValueList.add(attributeValueArray.get(j+1));
					}
					break;
				}
				if(j == attributeValueArray.size()-1){
					throw new SQLException("did not get the attribute value");
				}
			}			
		}
	}
	
	public void setAttributeValues(List<String> attributeNameList, List<String> attributeValueList) throws SQLException {
		for(int i = 0; i < this.attributeNameList.size(); i++){
			for(int j = 0; j < attributeNameList.size(); j++){
				if(this.attributeNameList.get(i).equals(attributeNameList.get(j))){
					//added by jiang yong 2015-04-26
					//for sql like: select * from item where i_id = 1
					this.attributeValueList.set(i, attributeValueList.get(j));
//					this.attributeValueList.add(attributeValueList.get(j));
					break;
				}
				if(j == this.attributeNameList.size()){
					throw new SQLException("did not get the attribute value");
				}
			}			
		}
	}

	public String toString(){
		return this.getPredicate();
	}

	@Override
	public QueryPredicate duplicate() {
		QueryPredicate tempQueryPredicate = new QueryPredicate(true);
		tempQueryPredicate.priority = this.priority;
		tempQueryPredicate.subQueryNumber = this.subQueryNumber;
		tempQueryPredicate.active = true;
		//not real duplicate:
		tempQueryPredicate.tableName = this.tableName;
		tempQueryPredicate.attributeNameList = this.attributeNameList;
		tempQueryPredicate.algebraicRelationshipList = this.algebraicRelationshipList;
		tempQueryPredicate.logicalRelationshipList = this.logicalRelationshipList;
		//added by jiang yong 2015-04-26
		//for sql like: select * from item where i_id = 1
		tempQueryPredicate.attributeValueList = this.attributeValueList;
		return tempQueryPredicate;
	}
}
