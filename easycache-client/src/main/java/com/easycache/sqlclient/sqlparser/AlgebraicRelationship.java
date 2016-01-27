package com.easycache.sqlclient.sqlparser;

enum AlgebraicRelationshipOperation {
	LIKE, GT, GE, LT, LE, EQUAL
}

public class AlgebraicRelationship{
	private AlgebraicRelationshipOperation algebraicRelationshipOperation;
	public AlgebraicRelationship(String strRelationship){
		if(strRelationship.equals("like") || strRelationship.equals("LIKE")){
			algebraicRelationshipOperation = AlgebraicRelationshipOperation.LIKE;
			return;
		}
		if(strRelationship.equals(">")){
			algebraicRelationshipOperation = AlgebraicRelationshipOperation.GT;
			return;
		}
		if(strRelationship.equals(">=")){
			algebraicRelationshipOperation = AlgebraicRelationshipOperation.GE;
			return;
		}
		if(strRelationship.equals("<")){
			algebraicRelationshipOperation = AlgebraicRelationshipOperation.LT;
			return;
		}
		if(strRelationship.equals("<=")){
			algebraicRelationshipOperation = AlgebraicRelationshipOperation.LE;
			return;
		}
		if(strRelationship.equals("=")){
			algebraicRelationshipOperation = AlgebraicRelationshipOperation.EQUAL;
			return;
		}
	}
	
	public String toString(){
		switch(algebraicRelationshipOperation){
		case LIKE: return "like";
		case GT: return ">";
		case GE: return ">=";
		case LT: return "<";
		case LE: return "<=";
		case EQUAL: return "=";
		}
		return null;
	}	
	public String toChangedString(){
		switch(algebraicRelationshipOperation){
		case LIKE: return "like";
		case GT: return "<";
		case GE: return "<=";
		case LT: return ">";
		case LE: return ">=";
		case EQUAL: return "=";
		}
		return null;
	}
}
