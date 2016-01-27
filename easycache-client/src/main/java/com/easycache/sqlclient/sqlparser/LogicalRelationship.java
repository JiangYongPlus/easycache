package com.easycache.sqlclient.sqlparser;

enum LogicalRelationshipOperation {
	AND, OR, NOT
}

public class LogicalRelationship{
	private LogicalRelationshipOperation logicalRelationshipOperation;
	public LogicalRelationship(String strLogicalRelationshipOperation){
		if(strLogicalRelationshipOperation.equals("and") || strLogicalRelationshipOperation.equals("AND")){
			logicalRelationshipOperation = LogicalRelationshipOperation.AND;
			return;
		}
		if(strLogicalRelationshipOperation.equals("or") || strLogicalRelationshipOperation.equals("OR")){
			logicalRelationshipOperation = LogicalRelationshipOperation.OR;
			return;
		}
		if(strLogicalRelationshipOperation.equals("not") || strLogicalRelationshipOperation.equals("NOT")){
			logicalRelationshipOperation = LogicalRelationshipOperation.NOT;
			return;
		}
	}
	
	public boolean equals(LogicalRelationshipOperation logicalRelationshipOperation){
		if(this.logicalRelationshipOperation.equals(logicalRelationshipOperation)){
			return true;
		}
		else{
			return false;
		}
	}
	
	public String toString(){
		switch(logicalRelationshipOperation){
		case AND: return "and";
		case OR: return "or";
		case NOT: return "not";
		}
		return null;
	}
}
