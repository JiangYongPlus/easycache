/*
 * jiang yong 2015.02
 * JSqlParserExecutor.java
 * parse sql and judge if the sql is supported or not through the tableName in the sql.
 */
package com.easycache.sqlclient.sqlparser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import com.easycache.sqlclient.HazelcastDatabaseMetaData;
import com.easycache.sqlclient.TableMetaData;
import com.easycache.sqlclient.load.Loader;
import com.easycache.sqlclient.log.IMDGLog;

public class JSqlParserExecutor {
	private HazelcastDatabaseMetaData hazelcastDatabaseMetaData = new HazelcastDatabaseMetaData();
	
	public boolean executeSelectPre(String sql, Statement statement) throws SQLException {
		
		SelectParserResult selectParserResult = SQLParserResultFactory.getSelectParserResult(sql);
		if (selectParserResult != null) {
			return selectParserResult.getIsSupportedSql();
		}
		
		Map<String, String> tableNameAliasMap;
		List<Item> itemList;
		List<Predicate> predicateList;
		
		selectParserResult = new SelectParserResult();
		tableNameAliasMap = new HashMap<String, String>();
		PlainSelect plainSelect = (PlainSelect)((Select) statement).getSelectBody();
		String tableName =  ((Table) plainSelect.getFromItem()).getName();
		tableName = (tableName == null ? tableName : tableName.toLowerCase());
		if (!Loader.isSupportedTable(tableName)) {
			selectParserResult.setIsSupportedSql(false);
			SQLParserResultFactory.setSelectParserResult(sql, selectParserResult);
			return false;
		}
		List <String> tableNameList = new ArrayList<String>();
		tableNameList.add(tableName);
		String tableNameAlias = ((Table) plainSelect.getFromItem()).getAlias();
		tableNameAlias = tableNameAlias == null ? tableNameAlias : tableNameAlias.toLowerCase();
		if(tableNameAlias==null){
			tableNameAliasMap.put(tableName, tableName);
		}
		else{
			tableNameAliasMap.put(tableNameAlias, tableName);
		}		
		if(plainSelect.getJoins() != null){
			int joinNumber = plainSelect.getJoins().size();
			for(int i = 0; i < joinNumber; i++){
				tableName = ((Table) ((Join) plainSelect.getJoins().get(i)).getRightItem()).getName();
				tableName = tableName == null ? tableName : tableName.toLowerCase();
				if (!Loader.isSupportedTable(tableName)) {
					selectParserResult.setIsSupportedSql(false);
					SQLParserResultFactory.setSelectParserResult(sql, selectParserResult);
					return false;
				}
				tableNameList.add(tableName);
				tableNameAlias = ((Join) plainSelect.getJoins().get(i)).getRightItem().getAlias();
				tableNameAlias = tableNameAlias == null ? tableNameAlias : tableNameAlias.toLowerCase();
				if(tableNameAlias==null){
					tableNameAliasMap.put(tableName, tableName);
				}
				else{
					tableNameAliasMap.put(tableNameAlias, tableName);
				}
			}
		}
		selectParserResult.setTableNameAliasMap(tableNameAliasMap);

		itemList = new ArrayList<Item>();
		@SuppressWarnings("rawtypes")
		List selectItems = plainSelect.getSelectItems();
		if(selectItems.size() == 1 && selectItems.get(0).toString().equals("*")){				
			for(String interTableName : tableNameAliasMap.keySet()){
				TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(interTableName);
				for(int j = 0; j < tableMetaData.getColumnSize(); j++){
					String attributeName = tableMetaData.getColumnName(j);
					Item item = new Item(interTableName+"."+attributeName);
					itemList.add(item);
				}
			}
		}
		else{
			for(int i = 0; i < selectItems.size(); i++){
				//jiang yong 2014-12-8
//				String tableAtrribute = hazelcastDatabaseMetaData.getTableAttribute(selectItems.get(i).toString());
				String tableAtrribute = hazelcastDatabaseMetaData.getTableAttribute(tableNameList, selectItems.get(i).toString());
				//done
				if(tableAtrribute != null){
					itemList.add(new Item(tableAtrribute.toLowerCase()));
				}
			}
		}
		selectParserResult.setItemList(itemList);

		Expression whereExpression = plainSelect.getWhere();
		if(whereExpression != null){
			WhereExpressionVisitor expressionVisitor = new WhereExpressionVisitor();
			//jiang yong 2014-12-8
			expressionVisitor.setTableNameList(tableNameList);
			//done
			whereExpression.accept(expressionVisitor);
			predicateList = expressionVisitor.getPredicateList();
		}
		else{
			predicateList = null;
		}
		selectParserResult.setPredicateList(predicateList);
		selectParserResult.setIsSupportedSql(true);
		SQLParserResultFactory.setSelectParserResult(sql, selectParserResult);
		return true;
	}

	public boolean executeInsertPre(String sql, Statement statement) throws SQLException {
		InsertParserResult insertParserResult = SQLParserResultFactory.getInsertParserResult(sql);
		if (insertParserResult != null) {
			return insertParserResult.getIsSupportedSql();
		}
		
		insertParserResult = new InsertParserResult();
		Insert insert = (Insert) statement;
		String tableName = insert.getTable().getName().toLowerCase();
		if (!Loader.isSupportedTable(tableName)) {
			insertParserResult.setIsSupportedSql(false);
			SQLParserResultFactory.setInsertParserResult(sql, insertParserResult);
			return false;
		}
		insertParserResult.setTableName(tableName);
		insertParserResult.setIsSupportedSql(true);
		SQLParserResultFactory.setInsertParserResult(sql, insertParserResult);
		return true;
	}

	public boolean executeDeletePre(String sql, Statement statement) throws SQLException{
		DeleteParserResult deleteParserResult = SQLParserResultFactory.getDeleteParserResult(sql);
		if (deleteParserResult != null) {
			return deleteParserResult.getIsSupportedSql();
		}
		deleteParserResult = new DeleteParserResult();
		QueryPredicate queryPredicate;
		Delete delete = (Delete) statement;
		String tableName = delete.getTable().getName().toLowerCase();
		if (!Loader.isSupportedTable(tableName)) {
			deleteParserResult.setIsSupportedSql(false);
			SQLParserResultFactory.setDeleteParserResult(sql, deleteParserResult);
			return false;
		}
		List <String> tableNameList = new ArrayList<String>();
		tableNameList.add(tableName);
		Expression whereExpression = delete.getWhere();
		if(whereExpression != null){
			WhereExpressionVisitor expressionVisitor = new WhereExpressionVisitor();
			//jiang yong 2014-12-25
			expressionVisitor.setTableNameList(tableNameList);
			//done
			whereExpression.accept(expressionVisitor);
			List<Predicate> predicateList = expressionVisitor.getPredicateList();
			if(!(predicateList.size() == 1)){
				throw new SQLException("where of delete sentence translated into several queries!");
			}
			if(!(predicateList.get(0) instanceof QueryPredicate) ){
				throw new SQLException("where of delete sentence translated into join query!");
			}
			queryPredicate = (QueryPredicate)predicateList.get(0);
		}
		else{
			throw new SQLException("delete without where!");
		}
		deleteParserResult.setTableName(tableName);
		deleteParserResult.setQueryPredicate(queryPredicate);
		deleteParserResult.setIsSupportedSql(true);
		SQLParserResultFactory.setDeleteParserResult(sql, deleteParserResult);	
		return true;
	}
	
	public boolean executeUpdatePre(String sql, Statement statement) throws SQLException{
		IMDGLog.print(sql);
		UpdateParserResult updateParserResult = SQLParserResultFactory.getUpdateParserResult(sql);
		if (updateParserResult != null) {
			return updateParserResult.getIsSupportedSql();
		}
		updateParserResult = new UpdateParserResult();
		QueryPredicate queryPredicate;
		Update update = (Update) statement;
		String tableName = update.getTable().getName().toLowerCase();
		if (!Loader.isSupportedTable(tableName)) {
			updateParserResult.setIsSupportedSql(false);
			SQLParserResultFactory.setUpdateParserResult(sql, updateParserResult);
			return false;
		}
		List<String> tableNameList = new ArrayList<String>();
		tableNameList.add(tableName);
		Expression whereExpression = update.getWhere();
		if(whereExpression != null){
			WhereExpressionVisitor expressionVisitor = new WhereExpressionVisitor();
			//jiang yong 2014-12-8
			expressionVisitor.setTableNameList(tableNameList);
			//done
			whereExpression.accept(expressionVisitor);
			List<Predicate> predicateList = expressionVisitor.getPredicateList();
			if(!(predicateList.size() == 1)){
				throw new SQLException("where of update sentence translated into several queries!");
			}
			if(!(predicateList.get(0) instanceof QueryPredicate) ){
				throw new SQLException("where of update sentence translated into join query!");
			}
			queryPredicate = (QueryPredicate)predicateList.get(0);
		}
		else{
			throw new SQLException("update without where!");
		}
		
		updateParserResult.setTableName(tableName);
		updateParserResult.setQueryPredicate(queryPredicate);
		updateParserResult.setIsSupportedSql(true);
		SQLParserResultFactory.setUpdateParserResult(sql, updateParserResult);
		
//		List<String> setAttributeNameList = new ArrayList<String>();
//		List<String> setAttributeValueList = new ArrayList<String>();
//
//		int columnSize = update.getColumns().size();
//		for (int i = 0; i < columnSize; i++) {
//			String attributeName = ((Column) update.getColumns().get(i)).getColumnName();
//			AttributeExpressionVisitor expressionVisitor = new AttributeExpressionVisitor();
//			((Expression) (update.getExpressions().get(i))).accept(expressionVisitor);
//			String attributeValue = expressionVisitor.getAttributeValue().toString();
//			setAttributeNameList.add(attributeName);
//			setAttributeValueList.add(attributeValue);
//		}
		return true;
	}
}
