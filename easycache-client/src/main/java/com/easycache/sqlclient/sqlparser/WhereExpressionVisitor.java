package com.easycache.sqlclient.sqlparser;

import java.util.ArrayList;
import java.util.List;

import com.easycache.sqlclient.HazelcastDatabaseMetaData;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

//jiang yong 2014-12-8
//if two tables hava the same column name, the function HazelcastDatabaseMetaData.getTableAttribute_backup()
// may get error, so we add tableNameList(for the join query) to class  WhereExpressionVisitor.
public class WhereExpressionVisitor implements ExpressionVisitor{
	private List<String> tableNameList = new ArrayList<String>();
	private int currentPriority = 0;
	List<Predicate> predicateList = new ArrayList<Predicate>();
	HazelcastDatabaseMetaData hazelcastDatabaseMetaData = new HazelcastDatabaseMetaData();
	
	public List<Predicate> getPredicateList(){
		return predicateList;
	}
	
	public void setTableNameList(List<String> tableNameList){
		this.tableNameList = tableNameList;
	}
	
	public List<String> getTableNameList(){
		return this.tableNameList;
	}
	
	public void visit(AndExpression andExpression) {
		andExpression.getLeftExpression().accept(this);
		andExpression.getRightExpression().accept(this);
	}
	
	public void visit(OrExpression orExpression) {
		orExpression.getLeftExpression().accept(this);
		orExpression.getRightExpression().accept(this);
	}
	
	public void visit(Parenthesis parenthesis) {
		currentPriority++;
		parenthesis.getExpression().accept(this);
	}
	
	public void visit(Between between) {
		// TODO Auto-generated method stub
	}
	
	public void visit(EqualsTo equalsTo) {
		// work around
		String tableAttributeOne = equalsTo.getLeftExpression().toString().toLowerCase();
		String tableAttributeTwo = equalsTo.getRightExpression().toString().toLowerCase();
		String attributeValue = equalsTo.getRightExpression().toString();
		tableAttributeOne = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeOne);
		tableAttributeTwo = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeTwo);

		String relationship = "=";
		if(tableAttributeOne != null && tableAttributeTwo != null){
			this.insertSubJoinPredicate(tableAttributeOne, relationship, tableAttributeTwo);
		}
		else{
			this.insertSubQueryPredicate(tableAttributeOne, relationship, attributeValue);
		}
	}
	
	public void visit(GreaterThan greaterThan) {
		String tableAttributeOne = greaterThan.getLeftExpression().toString().toLowerCase();
		String tableAttributeTwo = greaterThan.getRightExpression().toString().toLowerCase();
		String attributeValue = greaterThan.getRightExpression().toString();
		tableAttributeOne = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeOne);
		tableAttributeTwo = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeTwo);

		String relationship = ">";
		if(tableAttributeOne != null && tableAttributeTwo != null){
			this.insertSubJoinPredicate(tableAttributeOne, relationship, tableAttributeTwo);		
		}
		else{
			this.insertSubQueryPredicate(tableAttributeOne, relationship, attributeValue);
		}
	}
	
	public void visit(GreaterThanEquals greaterThanEquals) {
		String tableAttributeOne = greaterThanEquals.getLeftExpression().toString().toLowerCase();
		String tableAttributeTwo = greaterThanEquals.getRightExpression().toString().toLowerCase();
		String attributeValue = greaterThanEquals.getRightExpression().toString();
		tableAttributeOne = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeOne);
		tableAttributeTwo = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeTwo);

		String relationship = ">=";
		if(tableAttributeOne != null && tableAttributeTwo != null){
			this.insertSubJoinPredicate(tableAttributeOne, relationship, tableAttributeTwo);
		}
		else{
			this.insertSubQueryPredicate(tableAttributeOne, relationship, attributeValue);
		}
	}

	public void visit(InExpression inExpression) {
		// TODO Auto-generated method stub
	}

	public void visit(LikeExpression likeExpression) {
		String tableAttributeOne = likeExpression.getLeftExpression().toString().toLowerCase();
		String attributeValue = likeExpression.getRightExpression().toString();
		tableAttributeOne = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeOne);
				
		String relationship = "like";
		this.insertSubQueryPredicate(tableAttributeOne, relationship, attributeValue);
	}

	public void visit(MinorThan minorThan) {
		String tableAttributeOne = minorThan.getLeftExpression().toString().toLowerCase();
		String tableAttributeTwo = minorThan.getRightExpression().toString().toLowerCase();
		String attributeValue = minorThan.getRightExpression().toString();
		tableAttributeOne = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeOne);
		tableAttributeTwo = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeTwo);
				
		String relationship = "<";
		if(tableAttributeOne != null && tableAttributeTwo != null){
			this.insertSubJoinPredicate(tableAttributeOne, relationship, tableAttributeTwo);
		}
		else{
			this.insertSubQueryPredicate(tableAttributeOne, relationship, attributeValue);
		}
	}

	public void visit(MinorThanEquals minorThanEquals) {
		String tableAttributeOne = minorThanEquals.getLeftExpression().toString().toLowerCase();
		String tableAttributeTwo = minorThanEquals.getRightExpression().toString().toLowerCase();
		String attributeValue = minorThanEquals.getRightExpression().toString();
		tableAttributeOne = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeOne);
		tableAttributeTwo = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeTwo);
				
		String relationship = "<=";
		if(tableAttributeOne != null && tableAttributeTwo != null){
			this.insertSubJoinPredicate(tableAttributeOne, relationship, tableAttributeTwo);
		}
		else{
			this.insertSubQueryPredicate(tableAttributeOne, relationship, attributeValue);
		}
	}

	public void visit(NotEqualsTo notEqualsTo) {
		String tableAttributeOne = notEqualsTo.getLeftExpression().toString().toLowerCase();
		String tableAttributeTwo = notEqualsTo.getRightExpression().toString().toLowerCase();
		String attributeValue = notEqualsTo.getRightExpression().toString();
		tableAttributeOne = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeOne);
		tableAttributeTwo = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, tableAttributeTwo);
				
		String relationship = "!=";
		if(tableAttributeOne != null && tableAttributeTwo != null){
			this.insertSubJoinPredicate(tableAttributeOne, relationship, tableAttributeTwo);
		}
		else{
			this.insertSubQueryPredicate(tableAttributeOne, relationship, attributeValue);
		}
	}
	
	
	
	
	public void visit(Addition addition) {
		// TODO Auto-generated method stub
		
	}
	
	public void visit(Division division) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Subtraction subtraction) {
		// TODO Auto-generated method stub
		
	}
	
	public void visit(DoubleValue doubleValue) {
		// TODO Auto-generated method stub
		
	}

	public void visit(LongValue longValue) {
		// TODO Auto-generated method stub
		
	}

	public void visit(DateValue dateValue) {
		// TODO Auto-generated method stub
		
	}

	public void visit(TimeValue timeValue) {
		// TODO Auto-generated method stub
		
	}

	public void visit(TimestampValue timestampValue) {
		// TODO Auto-generated method stub
		
	}

	public void visit(StringValue stringValue) {
		// TODO Auto-generated method stub
		
	}
	
	public void visit(NullValue nullValue) {
		// TODO Auto-generated method stub
		
	}
	
	public void visit(Column tableColumn) {
		// TODO Auto-generated method stub
		
	}

	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub
		
	}

	public void visit(CaseExpression caseExpression) {
		// TODO Auto-generated method stub
		
	}

	public void visit(WhenClause whenClause) {
		// TODO Auto-generated method stub
		
	}

	public void visit(ExistsExpression existsExpression) {
		// TODO Auto-generated method stub
		
	}

	public void visit(AllComparisonExpression allComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	public void visit(AnyComparisonExpression anyComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Concat concat) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Matches matches) {
		// TODO Auto-generated method stub
		
	}

	public void visit(BitwiseAnd bitwiseAnd) {
		// TODO Auto-generated method stub
		
	}

	public void visit(BitwiseOr bitwiseOr) {
		// TODO Auto-generated method stub
		
	}

	public void visit(BitwiseXor bitwiseXor) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	public void visit(IsNullExpression isNullExpression) {
		// TODO Auto-generated method stub
		
	}
	
	public void visit(Function function) {
		// TODO Auto-generated method stub
		
	}

	public void visit(InverseExpression inverseExpression) {
		// TODO Auto-generated method stub
		
	}

	public void visit(JdbcParameter jdbcParameter) {
		// TODO Auto-generated method stub
		
	}
	
	
	private void insertSubJoinPredicate(String tableAttributeOne, String relationship, String tableAttributeTwo){
		for(int i = 0; i < predicateList.size(); i++){
			if(predicateList.get(i) instanceof QueryPredicate){
				continue;
			}
			JoinPredicate joinPredicate = ((JoinPredicate)predicateList.get(i)).addSubJoinPredicate("or", currentPriority, tableAttributeOne, relationship, tableAttributeTwo);
			if(joinPredicate != null){
				return;
			}
		}
		JoinPredicate joinPredicate = new JoinPredicate();
		joinPredicate.addSubJoinPredicate(null, currentPriority, tableAttributeOne, relationship, tableAttributeTwo);
		predicateList.add(joinPredicate);
	}
	
	private void insertSubQueryPredicate(String tableAttribute, String relationship, String attributeValue){
		for(int i = 0; i < predicateList.size(); i++){
			if(predicateList.get(i) instanceof JoinPredicate){
				continue;
			}
			QueryPredicate queryPredicate = ((QueryPredicate)predicateList.get(i)).addSubQueryPredicate("and", currentPriority, tableAttribute, relationship, attributeValue);
			if(queryPredicate != null){
				return;
			}
		}
		QueryPredicate queryPredicate = new QueryPredicate();
		queryPredicate.addSubQueryPredicate(null, currentPriority, tableAttribute, relationship, attributeValue);
		predicateList.add(queryPredicate);
	}
}

