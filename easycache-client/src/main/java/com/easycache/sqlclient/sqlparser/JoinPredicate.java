package com.easycache.sqlclient.sqlparser;

import java.util.ArrayList;
import java.util.List;

public class JoinPredicate extends Predicate{
	private String tableNameOne;
	private String tableNameTwo;	
	private List<String> attributeNameListOne;
	private List<AlgebraicRelationship> algebraicRelationshipList;
	private List<String> attributeNameListTwo;
	private List<LogicalRelationship> logicalRelationshipList;
	
	public JoinPredicate(){
		attributeNameListOne = new ArrayList<String>();
		algebraicRelationshipList = new ArrayList<AlgebraicRelationship>();
		attributeNameListTwo = new ArrayList<String>();
		logicalRelationshipList = new ArrayList<LogicalRelationship>();
	}
	
	public JoinPredicate(boolean bl){
		// it is useful, don't delete it!
	}
	
	public JoinPredicate addSubJoinPredicate(String strLogicalRelationship, int priority, String tableAtrributeOne, String relationship, String tableAttributeTwo){
		int index = tableAtrributeOne.indexOf(".");
		String generatedTableNameOne = tableAtrributeOne.substring(0,index);
		String generatedAttributeOne = tableAtrributeOne.substring(index+1);
		index = tableAttributeTwo.indexOf(".");
		String generatedTableNameTwo = tableAttributeTwo.substring(0,index);
		String generatedAttributeTwo = tableAttributeTwo.substring(index+1);
		return addSubJoinPredicate(strLogicalRelationship, priority, generatedTableNameOne, generatedAttributeOne, relationship, generatedTableNameTwo, generatedAttributeTwo);
	}
	
	public JoinPredicate addSubJoinPredicate(String strLogicalRelationship, int priority, String tableNameOne, String attributeNameOne, String relationship, String tableNameTwo, String attributeNameTwo){
		if(subQueryNumber == 0){
			this.priority = priority;
			this.tableNameOne = tableNameOne;
			this.tableNameTwo = tableNameTwo;
			attributeNameListOne.add(attributeNameOne);
			algebraicRelationshipList.add(new AlgebraicRelationship(relationship));
			attributeNameListTwo.add(attributeNameTwo);
			subQueryNumber++;
		}
		else{
			if(this.priority != priority){
				return null;
			}
			boolean bl = this.tableNameOne.equals(tableNameOne) && this.tableNameTwo.equals(tableNameTwo);
			if(bl == false){
				bl = this.tableNameOne.equals(tableNameTwo) && this.tableNameTwo.equals(tableNameOne);
			}
			if(!bl){
				return null;
			}
			if(strLogicalRelationship != null){
				this.logicalRelationshipList.add(new LogicalRelationship(strLogicalRelationship));
			}
			attributeNameListOne.add(attributeNameOne);
			algebraicRelationshipList.add(new AlgebraicRelationship(relationship));
			attributeNameListTwo.add(attributeNameTwo);
			subQueryNumber++;
		}
		return this;
	}	

	public int getPriority() {
		return priority;
	}
	
	public int getSubQueryNumber() {
		return subQueryNumber;
	}
	
	public String getTableNameOne() {
		return tableNameOne;
	}

	public String getTableNameTwo() {
		return tableNameTwo;
	}
	
	public String getAttributeOne(int index) {
		return attributeNameListOne.get(index);
	}

	public String getAttributeTwo(int index) {
		return attributeNameListTwo.get(index);
	}
	
	public String getAlgebraicRelationship(int index){
		return algebraicRelationshipList.get(index).toString();
	}	
	
	public String getChangedAlgebraicRelationship(int index){
		return algebraicRelationshipList.get(index).toChangedString();
	}

	public String getLogicalRelationship(int index){
		return logicalRelationshipList.get(index).toString();
	}
	
	public List<LogicalRelationship> getLogicalRelationshipList(){
		return logicalRelationshipList;
	}
	
	public String toString(){
		String predicate = "";
		for(int i = 0; i < subQueryNumber; i++){
			if(i==0){
				predicate += attributeNameListOne.get(i) + algebraicRelationshipList.get(i).toString() + attributeNameListTwo.get(i);
			}
			else{
				predicate += " " + logicalRelationshipList.get(i-1).toString() + " " + attributeNameListOne.get(i) + algebraicRelationshipList.get(i).toString() + attributeNameListTwo.get(i);
			}
		}
		return predicate;
	}

	@Override
	public JoinPredicate duplicate() {
		JoinPredicate tempJoinPredicate = new JoinPredicate(true);
		tempJoinPredicate.priority = this.priority;
		tempJoinPredicate.subQueryNumber = this.subQueryNumber;
		tempJoinPredicate.active = true;
		//not real
		tempJoinPredicate.tableNameOne = this.tableNameOne;
		tempJoinPredicate.tableNameTwo = this.tableNameTwo;	
		tempJoinPredicate.attributeNameListOne = this.attributeNameListOne;
		tempJoinPredicate.algebraicRelationshipList = this.algebraicRelationshipList;
		tempJoinPredicate.attributeNameListTwo = this.attributeNameListTwo;
		tempJoinPredicate.logicalRelationshipList = this.logicalRelationshipList;		
		return tempJoinPredicate;
	}
}

