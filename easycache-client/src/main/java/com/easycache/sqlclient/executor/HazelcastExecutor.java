package com.easycache.sqlclient.executor;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.cglib.beans.BeanGenerator;
import net.sf.cglib.beans.BeanMap;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.easycache.serialization.HazelcastObject;
import com.hazelcast.easycache.utility.IMDGString;
import com.hazelcast.query.SqlPredicate;
import com.easycache.sqlclient.BeanGeneratorFactory;
import com.easycache.sqlclient.HazelcastDatabaseMetaData;
import com.easycache.sqlclient.TableMetaData;
import com.easycache.sqlclient.jdbc.IMDGResultSet;
import com.easycache.sqlclient.sqlparser.DeleteParserResult;
import com.easycache.sqlclient.sqlparser.InsertParserResult;
import com.easycache.sqlclient.sqlparser.Item;
import com.easycache.sqlclient.sqlparser.JoinPredicate;
import com.easycache.sqlclient.sqlparser.Predicate;
import com.easycache.sqlclient.sqlparser.QueryPredicate;
import com.easycache.sqlclient.sqlparser.SQLParserResultFactory;
import com.easycache.sqlclient.sqlparser.SelectParserResult;
import com.easycache.sqlclient.sqlparser.UpdateParserResult;

public class HazelcastExecutor {
	
	private static BeanGeneratorFactory beanGeneratorFactory = new BeanGeneratorFactory();
	private HazelcastDatabaseMetaData hazelcastDatabaseMetaData = new HazelcastDatabaseMetaData();
	private static HazelcastInstance hazelcast = null;
	
	public HazelcastExecutor() {
		super();
		if((hazelcast = Hazelcast.getHazelcastInstanceByName("IMDG")) == null) {
			Config config = new XmlConfigBuilder().build();
			config.setInstanceName("IMDG");
			hazelcast = Hazelcast.newHazelcastInstance(config);
		}
	}
	
	//jiang yong 2014-11-28 for primaryKey is not only integer type
	public int executeInsert(String sql, List<String> attributeNameList, List<String> attributeValueList, IMDGResultSet hrs) throws SQLException {
		InsertParserResult insertParserResult = SQLParserResultFactory.getInsertParserResult(sql);
		if(insertParserResult == null){
			throw new SQLException("this insert sentence is not parsed, sql is : " + sql);
		}
		String tableName = insertParserResult.getTableName().toLowerCase();
		TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
		if(tableMetaData == null){
			throw new SQLException("table not exist, tableName is : " + tableName);
		}		
//      ArrayList<String> columnNameList = tableMetaData.getColumnNameList();
        @SuppressWarnings("rawtypes")
		List<Class> columnClassList = tableMetaData.getColumnClassList();
        List<String> columnTypeList = new ArrayList<String>();
        for(int i = 0; i < columnClassList.size(); i++){
        	columnTypeList.add(columnClassList.get(i).getName());
        }
//      hrs.addMetaData(columnNameList,columnTypeList, null, null);
        
		BeanGenerator beanGenerator = beanGeneratorFactory.getBeanGenerator(tableName);
		if(beanGenerator == null){
			/*
			beanGenerator = new BeanGenerator();
			beanGenerator.setSuperclass(HazelcastObject.class);
			int columnSize = insert.getColumns().size();
			for(int i = 0; i < columnSize; i ++){
				String attributeName = ((Column) insert.getColumns().get(i)).getColumnName();
				Class myClass = ((ExpressionList) insert.getItemsList()).getExpressions().get(i).getClass();
				beanGenerator.addProperty(attributeName,
						((Column) insert.getColumns().get(i)).getClass());
			}
			beanGeneratorFactory.setBeanGenerator(tableName, beanGenerator);
			*/
			throw new SQLException("there is no generator!");
		}

		HazelcastObject hazelcastObject = (HazelcastObject) beanGenerator.create();
		BeanMap beanMap = BeanMap.create(hazelcastObject);
		int columnSize = attributeNameList.size();
		String attributeName;
		String attributeValueStr;
		Object attributeValueObj = null;
		//TODO: the code here should be updated with the code in LoaderHelper, because their logic are the same.
		//jiang yong
		for(int i = 0; i < columnSize; i ++){
			attributeName = attributeNameList.get(i).toLowerCase();
			attributeValueStr = attributeValueList.get(i);	
			if(attributeValueStr == null){
				attributeValueObj = null;
			}
			else{
				if(attributeValueStr.startsWith("'")) {
					attributeValueStr = attributeValueStr.substring(1,attributeValueStr.length() - 1);
				}
				
				@SuppressWarnings("rawtypes")
				Class columnClass = tableMetaData.getColumnClassByAttributeName(attributeName);
				if(columnClass.equals(Integer.class)){
					attributeValueObj = Integer.parseInt(attributeValueStr);
				} else if(columnClass.equals(String.class)){
					attributeValueObj = String.valueOf(attributeValueStr);
				} else if(columnClass.equals(java.sql.Date.class)){
					attributeValueObj = java.sql.Date.valueOf(attributeValueStr);
				} else if(columnClass.equals(java.sql.Timestamp.class)){
//					System.out.println("str: " + attributeValueStr);
//					DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//					String timeStamp = df.format(attributeValueStr);
//					attributeValueObj = java.sql.Timestamp.valueOf(timeStamp);
					attributeValueObj = java.sql.Timestamp.valueOf(attributeValueStr);
				} else if(columnClass.equals(java.math.BigDecimal.class)){
					attributeValueObj = new BigDecimal(attributeValueStr);
				} else if(columnClass.equals(Double.class)) {
					attributeValueObj = Double.valueOf(attributeValueStr);
				}
				else{
					System.out.println("Warning: unsupported columnClass:" + columnClass);
				}
			}
			beanMap.put(attributeName, attributeValueObj);
		}
		
		IdGenerator idGenerator = hazelcast.getIdGenerator(tableName);
		long id = 0;
		String strid;
		if(tableMetaData.getPrimaryKeyListSize() == 1){
			String primaryAttributeName = tableMetaData.getPrimaryKeyList().get(0);
    		@SuppressWarnings("rawtypes")
			Class attributeClass = tableMetaData.getColumnClassByAttributeName(primaryAttributeName);
    		if(beanMap.get(primaryAttributeName) == null && (attributeClass.equals(Integer.class) || attributeClass.equals(java.math.BigDecimal.class))){
    			id = idGenerator.newId();
    			strid =  primaryAttributeName + "=";    		
        		strid += String.valueOf(id); 
        		hrs.addPrimaryKey(id, Long.class);
        		Object primaryIdObj = null;
        		if(attributeClass.equals(Integer.class)){
    				primaryIdObj = Integer.valueOf((int)id);
    			}else if(attributeClass.equals(java.math.BigDecimal.class)){
    				primaryIdObj = new BigDecimal(id);
    			}
    			else{
    				throw new SQLException("not supported data type for primary key!");
    			}
        		beanMap.put(primaryAttributeName, primaryIdObj);
    		}
    		else{
    			strid = "";
    			strid += primaryAttributeName + "=" + "'" + beanMap.get(primaryAttributeName) + "'";		
    		}
		}
		else{
			strid = "";
			for(String primaryAttributeName : tableMetaData.getPrimaryKeyList()){
				strid += primaryAttributeName + "=" + beanMap.get(primaryAttributeName)+"$#@";				
			}
		}
		strid = tableName + IMDGString.TABLE_TAG + strid;
//		System.out.println(strid);
		hazelcastObject.setId(strid);		
		IMap<String, Object> myMap = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
		//jiang yong 2014-12-11
		//for repeated primary key when insert operation, the output format is like mysql
		try{
			if(myMap.get(strid) != null){
				String [] strids = strid.split("\\$#@");
				String strkey = "";
				for(int i = 0; i < (strids.length - 1); i++){
					strkey = strkey.concat(strids[i].substring(strids[i].indexOf("=") + 1)).concat("-");
				}
				strkey = strkey.concat(strids[strids.length-1].substring(strids[strids.length-1].indexOf("=") + 1));
				throw new Exception("Duplicate entry " + strkey + " for primary key");
			}
			myMap.put(strid, hazelcastObject);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// jiang yong done
		return 1;
	}
	
	
	public int executeDelete(String sql, List<String> attributeNameList, List<String> attributeValueList) throws SQLException{
		DeleteParserResult deleteParserResult = SQLParserResultFactory.getDeleteParserResult(sql);
		if(deleteParserResult == null){
			throw new SQLException("this delete sentence is not cached, sql is : " + sql);
		}
		String tableName = deleteParserResult.getTableName();
		QueryPredicate queryPredicate = deleteParserResult.getQueryPredicate().duplicate();	
		queryPredicate.setAttributeValues(attributeNameList, attributeValueList);
		queryPredicate.setActive();
		IMap<String, HazelcastObject> myMap = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
		TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
		if(tableMetaData == null){
			throw new SQLException("table not exist, tableName is : " + tableName);
		}
		String requiredKey = queryPredicate.getRequiredKey(tableMetaData.getPrimaryKeyList());			
		Set<String> toDeleteKeySet = null;
		if(requiredKey != null){
			// jiang yong 2015-06-14
			// add tableName, tableName is for persistence
			myMap.remove(tableName + IMDGString.TABLE_TAG + requiredKey);
//			myMap.remove(requiredKey);
			return 1;
		}
		else{
			toDeleteKeySet = myMap.keySet(new SqlPredicate(queryPredicate.getPredicate()));
			for(String toDeleteKey : toDeleteKeySet){
				myMap.remove(tableName + IMDGString.TABLE_TAG + toDeleteKey);
//				myMap.remove(toDeleteKey);
			}
			return toDeleteKeySet.size();
		}
	}	

	public int executeUpdate(String sql, List<String> setAttributeNameList, List<String> setAttributeValueList, List<String> whereAttributeNameList, List<String> whereAttributeValueList) throws SQLException{
		UpdateParserResult updateParserResult = SQLParserResultFactory.getUpdateParserResult(sql);
		if(updateParserResult == null){
			throw new SQLException("this update sentence is not cached!");
		}
		String tableName = updateParserResult.getTableName();
		QueryPredicate queryPredicate = updateParserResult.getQueryPredicate().duplicate();	
		queryPredicate.setAttributeValues(whereAttributeNameList, whereAttributeValueList);
		queryPredicate.setActive();
		IMap<String, HazelcastObject> myMap = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
		TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
		if(tableMetaData == null){
			throw new SQLException("table not exist!");
		}
		String requiredKey = queryPredicate.getRequiredKey(tableMetaData.getPrimaryKeyList());			
		Collection<HazelcastObject> toUpdateOnes = null;
		if(requiredKey != null){
			toUpdateOnes = new HashSet<HazelcastObject>();
			// jiang yong 2015-06-14
			// add tableName, tableName is for persistence
			Object object= myMap.get(tableName + IMDGString.TABLE_TAG + requiredKey);
			if(object instanceof HazelcastObject){
				toUpdateOnes.add((HazelcastObject) object);
			}
		}
		else{
			toUpdateOnes = myMap.values(new SqlPredicate(queryPredicate.getPredicate()));
		}

        @SuppressWarnings("rawtypes")
		List<Class> columnClassList = tableMetaData.getColumnClassList();
        List<String> columnTypeList = new ArrayList<String>();
        for(int i = 0; i < columnClassList.size(); i++){
        	columnTypeList.add(columnClassList.get(i).getName());
        }

		for(HazelcastObject hazelcastObject : toUpdateOnes){
			BeanMap beanMap = BeanMap.create(hazelcastObject);
			String attributeName;
			String attributeValueStr;
			Object attributeValueObj = null;
			int columnSize = setAttributeNameList.size();
			for(int i = 0; i < columnSize; i ++){
				attributeName = setAttributeNameList.get(i);
				attributeValueStr = setAttributeValueList.get(i);
				@SuppressWarnings("rawtypes")
				Class columnClass = tableMetaData.getColumnClassByAttributeName(attributeName);
				if(attributeValueStr.startsWith("'")) {
					attributeValueStr = attributeValueStr.substring(1,attributeValueStr.length() - 1);
				}
				if(columnClass.equals(Integer.class)){
					attributeValueObj = Integer.parseInt(attributeValueStr);
				} else if(columnClass.equals(String.class)){
					attributeValueObj = String.valueOf(attributeValueStr);
				} else if(columnClass.equals(java.sql.Date.class)){
					attributeValueObj = java.sql.Date.valueOf(attributeValueStr);
				} else if(columnClass.equals(java.sql.Timestamp.class)){
					attributeValueObj = java.sql.Timestamp.valueOf(attributeValueStr);
				} else if(columnClass.equals(java.math.BigDecimal.class)){
					attributeValueObj = new BigDecimal(attributeValueStr);
				} else if(columnClass.equals(Double.class) ){ 
					attributeValueObj = new Double(attributeValueStr);
				}
				else{
					System.out.println("Warning: unsupported columnClass:" + columnClass);
				}
				beanMap.put(attributeName.toLowerCase(), attributeValueObj);
			}
			myMap.put(hazelcastObject.getId(), hazelcastObject);
		}
		return toUpdateOnes.size();
	}
	

	
	public int executeSelect(String key, List<String> attributeNameList, List<String> attributeValueList, IMDGResultSet hrs) throws SQLException {
		Map<String, String> tableNameAliasMap;
		List<Item> itemList;
		List<Predicate> predicateList;
		
		SelectParserResult selectParserResult = SQLParserResultFactory.getSelectParserResult(key);
		if(selectParserResult != null){
			tableNameAliasMap = selectParserResult.getTableNameAliasMap();
			itemList = selectParserResult.getItemList();
			predicateList = selectParserResult.duplicatePredicateList(attributeNameList, attributeValueList);
			if(predicateList == null && tableNameAliasMap.size() == 1){
				String tableNameString = tableNameAliasMap.keySet().toArray()[0].toString();
				this.executeQueryPredicate(itemList, tableNameString, hrs);
				return 0;
			}
			if(predicateList == null && tableNameAliasMap.size() > 1){
				throw new SQLException("not supported now");
			}		
			
			
			QueryPredicate queryPredicate = null;		
			for(int i = 0; i < predicateList.size(); i++){
				if((predicateList.get(i) instanceof QueryPredicate) && predicateList.get(i).isActive()){
					queryPredicate = (QueryPredicate) predicateList.get(i);
					predicateList.get(i).setInactive();
					break;
				}
			}
			if(queryPredicate == null){
				throw new SQLException("there is no query predicate, not supported now");
			}
			Collection<HazelcastObject> currentResultSet = this.executeQueryPredicate(itemList, queryPredicate, tableNameAliasMap, hrs);
			if(currentResultSet == null || currentResultSet.size() == 0){
				hrs.clear();
				return 0;
			}
			String currentTableName = queryPredicate.getTableName();
			int activePredicateNumber = 0; 
			for(int i = 0; i < predicateList.size(); i++){
				if(predicateList.get(i).isActive()){
					activePredicateNumber++;
				}
			}
			for(int i = 0; i < activePredicateNumber; i++){
				JoinPredicate joinPredicate = getNextJoinPredicate(predicateList, currentTableName);
				currentResultSet = this.executeJoinPredicate(itemList, joinPredicate, currentResultSet, currentTableName, tableNameAliasMap, hrs);
				if(currentResultSet == null || currentResultSet.size() == 0){
					hrs.clear();
					return 0;
				}
				currentTableName = (joinPredicate.getTableNameOne().equals(currentTableName) ? joinPredicate.getTableNameTwo() : joinPredicate.getTableNameOne());
			}
			return 0;
		}
		else{
			throw new SQLException("this sentence is not cached!");
		}
	}	
	
	private void executeQueryPredicate(List<Item> itemList, String tableName,  IMDGResultSet hrs){		
		//System.out.println("the query predicate is : no where predicate");
		TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
		IMap<String, HazelcastObject> myMap = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
		Collection<HazelcastObject> resultSet = myMap.values();
		
		List<String> requiredAttributeNameList = new ArrayList<String>();
		for(Item item : itemList){
			if(item.getTableName().equals(tableName)){
				String attributeName = item.getAttributeName();
				@SuppressWarnings("rawtypes")
				Class attributeClass = tableMetaData.getColumnClassByAttributeName(attributeName);
				requiredAttributeNameList.add(attributeName);
				hrs.addMetaDataCol(attributeName, attributeClass, tableName, null);
			}
		}
		
		List<List<Object>> partialResultList = new ArrayList<List<Object>>();
		for(int i = 0; i < requiredAttributeNameList.size(); i++){
			partialResultList.add(new ArrayList<Object>());
		}
		for(HazelcastObject hazelcastObject : resultSet){
			for(int i = 0; i < requiredAttributeNameList.size(); i++){
				String requiredAttributeName = requiredAttributeNameList.get(i);
				List<Object> columnList = partialResultList.get(i);
				Field field = null;				
				try {
					field = hazelcastObject.getClass().getDeclaredField("$cglib_prop_"+requiredAttributeName);
					field.setAccessible(true);
					columnList.add(field.get(hazelcastObject));
					//System.out.println(requiredAttributeName + "\t" + field.get(hazelcastObject));
				} catch (SecurityException e) {
					e.printStackTrace();
				} catch (NoSuchFieldException e) {
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}		
		
		for(int i = 0; i < partialResultList.size(); i++){
			try {
				hrs.addValueDataColumn(partialResultList.get(i));
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}	
	}	
	
	private Collection<HazelcastObject> executeQueryPredicate(List<Item> itemList, QueryPredicate queryPredicate, Map<String, String> tableNameAliasMap, IMDGResultSet hrs) throws SQLException{		
		//System.out.println("the query predicate is : " + queryPredicate);
		String tableName = queryPredicate.getTableName();
		String realTableName = tableNameAliasMap.get(tableName);
		IMap<String, HazelcastObject> myMap = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + realTableName);
		TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(realTableName);
		String requiredKey = queryPredicate.getRequiredKey(tableMetaData.getPrimaryKeyList());
		ArrayList<String> requiredKeyList = requiredKey == null ? queryPredicate.getRequiredKeyList(tableMetaData.getPrimaryKeyList()):null;

		Collection<HazelcastObject> resultSet = new HashSet<HazelcastObject>();
		if(requiredKey != null){
			resultSet = new HashSet<HazelcastObject>();
			// jiang yong 2015-06-14
			// add tableName, tableName is for persistence
			Object object= myMap.get(realTableName + IMDGString.TABLE_TAG + requiredKey);
//			Object object= myMap.get(requiredKey);
			if(object instanceof HazelcastObject){
				resultSet.add((HazelcastObject) object);
			}
		}
		else if(requiredKeyList != null){
			resultSet = new HashSet<HazelcastObject>();
			for(String key : requiredKeyList){
				Object object= myMap.get(realTableName + IMDGString.TABLE_TAG + key);
				if(object instanceof HazelcastObject){
					resultSet.add((HazelcastObject) object);
				}
			}
		}
		else{
			SqlPredicate sqlPredicate = new SqlPredicate(queryPredicate.getPredicate());
			Set<HazelcastObject> collection = (Set<HazelcastObject>) myMap.values(sqlPredicate);
			Iterator<HazelcastObject> it = (Iterator<HazelcastObject>) collection.iterator();
			while(it.hasNext()) {
				HazelcastObject object = it.next();
				resultSet.add(object);
			}
		}
		
		List<String> requiredAttributeNameList = new ArrayList<String>();
		for(Item item : itemList){
			if(item.getTableName().equals(tableName)){
				String attributeName = item.getAttributeName();
				@SuppressWarnings("rawtypes")
				Class attributeClass = tableMetaData.getColumnClassByAttributeName(attributeName);
				requiredAttributeNameList.add(attributeName);
				hrs.addMetaDataCol(attributeName, attributeClass, realTableName, null);
			}
		}
		
		List<List<Object>> partialResultList = new ArrayList<List<Object>>();
		for(int i = 0; i < requiredAttributeNameList.size(); i++){
			partialResultList.add(new ArrayList<Object>());
		}		

		for(HazelcastObject hazelcastObject : resultSet){
			for(int i = 0; i < requiredAttributeNameList.size(); i++){
				String requiredAttributeName = requiredAttributeNameList.get(i);
				List<Object> columnList = partialResultList.get(i);
				Field field = null;				
				try {
					field = hazelcastObject.getClass().getDeclaredField("$cglib_prop_"+requiredAttributeName);
					field.setAccessible(true);					
					columnList.add(field.get(hazelcastObject));	
					//System.out.println(requiredAttributeName + "\t" + field.get(hazelcastObject));
				} catch (SecurityException e) {
					e.printStackTrace();
				} catch (NoSuchFieldException e) {
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
		
		for(int i = 0; i < partialResultList.size(); i++){
			hrs.addValueDataColumn(partialResultList.get(i));
		}		

		return resultSet;
	}
	
	private Collection<HazelcastObject> executeQueryPredicate(QueryPredicate queryPredicate, Map<String, String> tableNameAliasMap) throws SQLException{		
		//System.out.println("the query predicate is : " + queryPredicate);
		String tableName = queryPredicate.getTableName();
		String realTableName = tableNameAliasMap.get(tableName);
		IMap<String, HazelcastObject> myMap = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + realTableName);
		TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(realTableName);
		String requiredKey = queryPredicate.getRequiredKey(tableMetaData.getPrimaryKeyList());			
		Collection<HazelcastObject> resultSet = null;
		if(requiredKey != null){
			resultSet = new HashSet<HazelcastObject>();
			// jiang yong 2015-06-14
			// add tableName, tableName is for persistence
			Object object= myMap.get(realTableName + IMDGString.TABLE_TAG + requiredKey);
//			Object object= myMap.get(requiredKey);
			if(object instanceof HazelcastObject){
				resultSet.add((HazelcastObject) object);
			}
		}
		else{
			resultSet = myMap.values(new SqlPredicate(queryPredicate.getPredicate()));
		}
		return resultSet;
	}
	
	
	private JoinPredicate getNextJoinPredicate(List<Predicate> predicateList, String currentTableName) throws SQLException{
		for(int i = 0; i < predicateList.size(); i++){
			if((predicateList.get(i) instanceof QueryPredicate) && predicateList.get(i).isActive()){
				throw new SQLException("doesn't support now");
			}
			else{
				if(!predicateList.get(i).isActive()){
					continue;
				}
				if(((JoinPredicate) predicateList.get(i)).getTableNameOne().equals(currentTableName) ||
						((JoinPredicate) predicateList.get(i)).getTableNameTwo().equals(currentTableName) ){
					JoinPredicate joinPredicate = (JoinPredicate) predicateList.get(i);
					predicateList.get(i).setInactive();
					return joinPredicate;
				}
			}
		}
		throw new SQLException("doesn't support now");
	}
	
	private Collection<HazelcastObject> executeJoinPredicate(List<Item> itemList, JoinPredicate joinPredicate, Collection<HazelcastObject> currentResultSet, 
			String currentTableName, Map<String, String> tableNameAliasMap, IMDGResultSet hrs) throws SQLException{		
		//System.out.println("the join predicate is : " + joinPredicate);
		Collection<HazelcastObject> resultSet;
		List<HazelcastObject> resultSetList = new ArrayList<HazelcastObject>();
		
		String tableName = "";
		for(HazelcastObject hazelcastObject : currentResultSet){
			QueryPredicate queryPredicate = this.generateQueryPredicate(joinPredicate, currentTableName, hazelcastObject);
			if(currentResultSet.size()<=1){
				resultSet = this.executeQueryPredicate(itemList, queryPredicate, tableNameAliasMap, hrs);
				return resultSet;
			}
			tableName = queryPredicate.getTableName();
			Collection<HazelcastObject> tempResultSet = this.executeQueryPredicate(queryPredicate, tableNameAliasMap);
			resultSetList.addAll(tempResultSet);
		}
		
		String realTableName = tableNameAliasMap.get(tableName);
		TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(realTableName);
		List<String> requiredAttributeNameList = new ArrayList<String>();
		for(Item item : itemList){
			if(item.getTableName().equals(tableName)){
				String attributeName = item.getAttributeName();
				@SuppressWarnings("rawtypes")
				Class attributeClass = tableMetaData.getColumnClassByAttributeName(attributeName);
				requiredAttributeNameList.add(attributeName);
				hrs.addMetaDataCol(attributeName, attributeClass, realTableName, null);
			}
		}
		
		List<List<Object>> partialResultList = new ArrayList<List<Object>>();
		for(int i = 0; i < requiredAttributeNameList.size(); i++){
			partialResultList.add(new ArrayList<Object>());
		}		

		for(HazelcastObject hazelcastObject : resultSetList){
			for(int i = 0; i < requiredAttributeNameList.size(); i++){
				String requiredAttributeName = requiredAttributeNameList.get(i);
				List<Object> columnList = partialResultList.get(i);
				Field field = null;				
				try {
					field = hazelcastObject.getClass().getDeclaredField("$cglib_prop_"+requiredAttributeName);
					field.setAccessible(true);					
					columnList.add(field.get(hazelcastObject));	
					//System.out.println(requiredAttributeName + "\t" + field.get(hazelcastObject));
				} catch (SecurityException e) {
					e.printStackTrace();
				} catch (NoSuchFieldException e) {
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
		
		for(int i = 0; i < partialResultList.size(); i++){
			hrs.addValueDataColumn(partialResultList.get(i));
		}

		return resultSetList;
	}
	
	
	private QueryPredicate generateQueryPredicate(JoinPredicate joinPredicate, String currentTableName, HazelcastObject hazelcastObject){
		QueryPredicate queryPredicate = new QueryPredicate();
		int priority = joinPredicate.getPriority();
		String tableName = null;
		String attributeName = null;
		String relationship = null;
		String tempAttributeName = null;
		String attributeValue = null;
		for(int i = 0; i < joinPredicate.getSubQueryNumber(); i++){
			if(joinPredicate.getTableNameOne().equals(currentTableName)){
				tableName = joinPredicate.getTableNameTwo();
				attributeName = joinPredicate.getAttributeTwo(i);
				relationship = joinPredicate.getChangedAlgebraicRelationship(i);
				tempAttributeName = joinPredicate.getAttributeOne(i);
			}
			else{
				tableName = joinPredicate.getTableNameOne();
				attributeName = joinPredicate.getAttributeOne(i);
				relationship = joinPredicate.getAlgebraicRelationship(i);
				tempAttributeName = joinPredicate.getAttributeOne(i);
			}
			
			Field field = null;				
			try {
				field = hazelcastObject.getClass().getDeclaredField("$cglib_prop_"+tempAttributeName);
				field.setAccessible(true);
				attributeValue = field.get(hazelcastObject).toString();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			queryPredicate.addSubQueryPredicate(null, priority, tableName, attributeName, relationship, attributeValue);
		}
		queryPredicate.setLogicalRelationshipList(joinPredicate.getLogicalRelationshipList());
		return queryPredicate;
	}
}

