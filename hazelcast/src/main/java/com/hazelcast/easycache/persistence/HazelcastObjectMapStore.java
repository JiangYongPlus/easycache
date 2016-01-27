/*
 * jiang yong 2015-06-11
 * for HazelcastObject persistence
 */
package com.hazelcast.easycache.persistence;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.core.MapStore;
import com.hazelcast.easycache.serialization.HazelcastObject;
import com.hazelcast.easycache.utility.ConnectionPool;
import com.hazelcast.easycache.utility.IMDGString;

public class HazelcastObjectMapStore implements MapStore<String, HazelcastObject> {
	
	private Map<String, SQLParser> psps  = null;
	private static boolean enable = false;
	public HazelcastObjectMapStore(){
		psps = new ConcurrentHashMap<String, SQLParser>();
	}
	
    //jiang yong 2015-07-01
    //when load data, map can not persistence, but when insert, update and delete, map should persistence
	public static void setEnalbe(boolean flag) {
		enable = flag;
	}
	
	public static boolean getEnable() {
		return enable;
	}
	//jiang yong done
	
	@Override
	public void store(String key, HazelcastObject value) {
		int index = key.indexOf(IMDGString.TABLE_TAG);
		String tableName = key.substring(0, index);
		key = key.substring(index + IMDGString.TABLE_TAG.length());
//		System.out.println("start to store");
		SQLParser psp = psps.get(tableName);
    	if(psp == null){
			psp = psps.get(tableName);
			if (psp == null) {
				psp = new SQLParser();
				psps.put(tableName, psp);
    		}
    	}
    	
    	if(value != null){
    		String sql = psp.getQuerySQLFromObject((String)key, value, tableName);
//        	System.out.println("persistence preDefaultMapStore:" + sql);
        	Connection con = ConnectionPool.getConnection();
        	PreparedStatement stmt = null;
        	PreparedStatement stmt1 = null;
        	try {
        		stmt = con.prepareStatement(sql);
        		
        		String k = (String)key;
        		String [] keys = k.split(SQLParser.SPLIT_LABEL);
    			if(keys == null || keys.length == 1){
    				String tempStr = k.substring(k.indexOf("=")+1);
    				if(tempStr.contains("'")){
    					stmt.setString(1, tempStr.substring(1, tempStr.length()-1));
    				}
    				else{
    					stmt.setInt(1, Integer.valueOf(tempStr));
    				}
//    				System.out.println("Insert Col Value:" + k.substring(k.indexOf("=")+1));
    			} else {
    				ArrayList<String> cols = psp.getQueryCols();
    				String tempStr = keys[0].substring(keys[0].indexOf("=")+1);
    				if(tempStr.contains("'")){
    					stmt.setString(cols.indexOf(keys[0].substring(0, keys[0].indexOf("=")))+1, tempStr.substring(1, tempStr.length()-1));
    				}
    				else{
    					stmt.setInt(cols.indexOf(keys[0].substring(0, keys[0].indexOf("=")))+1, Integer.valueOf(tempStr));
    				}
    				for(int i = 1; i != keys.length; ++i){
    					tempStr = keys[i].substring(keys[i].indexOf("=")+1);
    					if(tempStr.contains("'")){
        					stmt.setString(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, tempStr.substring(1, tempStr.length()-1));
        				}
        				else{
        					stmt.setInt(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, Integer.valueOf(tempStr));
        				}
//    					stmt.setObject(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, 
//    							keys[i].substring(keys[i].indexOf("=")+2, keys[i].length()-1));
//    					stmt.setInt(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, 
//    							Integer.valueOf(keys[i].substring(keys[i].indexOf("=")+1)));
    				}
    			}
    			
    			ResultSet rs = stmt.executeQuery();
    			if(rs != null && !rs.next()){
    				sql = psp.getInsertSQLFromObject(value, tableName);
//    				System.out.println(sql);
    				stmt1 = con.prepareStatement(sql);
    				ArrayList<String> cols = psp.getInsertCols();
//    				int index;
//    				Object object;
    				for(Field field : value.getClass().getDeclaredFields()){
    					field.setAccessible(true);
    					String name = field.getName();
    					if(name.startsWith(SQLParser.PREFIX))
    						name = name.substring(SQLParser.PREFIX_LEN);
    					if(field.get(value) != null){
//    						index = cols.indexOf(name)+1;
//    						object = null;
    						stmt1.setObject(cols.indexOf(name)+1, field.get(value));	
    					} else {
//    						index = cols.indexOf(name)+1;
//    						object = null;
    						stmt1.setObject(cols.indexOf(name)+1, null);
    					}
//    					System.out.println(sql + "[" + index + ":" + object + "]");
    				}
    				
    				stmt1.executeUpdate();
//    				insert(key, value, tableName, con);
    			} else {
    				sql = psp.getUpdateSQLFromObject((String)key, value, tableName);
    				stmt1 = con.prepareStatement(sql);
    				
    				ArrayList<String> cols = psp.getUpdateCols();
//    				int index;
//    				Object object;
    				for(Field field : value.getClass().getDeclaredFields()){
    					field.setAccessible(true);
    					String name = field.getName();
    					if(name.startsWith(SQLParser.PREFIX))
    						name = name.substring(SQLParser.PREFIX_LEN);
    					if(field.get(value) != null){
//    						index = cols.indexOf(name)+1;
//    						object = field.get(value);
    						stmt1.setObject(cols.indexOf(name)+1, field.get(value));	
    					} else {
//    						index = cols.indexOf(name)+1;
//    						object = null;
    						stmt1.setObject(cols.indexOf(name)+1, null);
    					}
//    					System.out.println(sql + "[" + index + ":" + object + "]");
    				}
    				
    				stmt1.executeUpdate();
//    				update(key, value, tableName, con);
    			}
    		} catch (SQLException e) {
    			long threadId = Thread.currentThread().getId();
				System.out.println("==============================[" + threadId + "] Debug Info ==============================");
				System.out.println("[" + threadId +"] SQL:" + sql);
				for(Field field : value.getClass().getDeclaredFields()){
					field.setAccessible(true);
					String name = field.getName();
					try {
						System.out.println("[" + threadId + "] " + name + " : " + field.get(value));
					} catch (IllegalArgumentException e1) {
						e1.printStackTrace();
					} catch (IllegalAccessException e1) {
						e1.printStackTrace();
					}
				}
				System.out.println("================================" + threadId + "====================================");
    			e.printStackTrace();
    		} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} finally {
    			closeStmt(stmt);
    			closeStmt(stmt1);
    			closeConnection(con);
    		}
    	}
	}

	@Override
	public void storeAll(Map<String, HazelcastObject> map) {
		 Set<Entry<String, HazelcastObject>> entrys = map.entrySet();
	        for(Entry<String, HazelcastObject> entry : entrys){
	        	store(entry.getKey(), entry.getValue());
	        }
	}

	@Override
	public void delete(String key) {
		int index = key.indexOf(IMDGString.TABLE_TAG);
		String tableName = key.substring(0, index);
		key = key.substring(index + IMDGString.TABLE_TAG.length());
		SQLParser psp = psps.get(tableName);
     	if(psp == null){
			psp = psps.get(tableName);
			if (psp == null) {
				psp = new SQLParser();
				psps.put(tableName, psp);
    		}
    	}
    	
    	String sql = psp.getDeleteSQLFromObject((String)key, tableName);
//    	System.out.println(sql);
    	Connection con = ConnectionPool.getConnection();
    	PreparedStatement stmt = null;
    	try {
    		stmt = con.prepareStatement(sql);
    		String k = (String)key;
    		String [] keys = k.split(SQLParser.SPLIT_LABEL);
			if(keys == null || keys.length == 1){
//				stmt.setInt(1, Integer.valueOf(k.substring(k.indexOf("=")+1)));
//				stmt.setObject(1, k.substring(k.indexOf("=")+1));
				String tempStr = k.substring(k.indexOf("=")+1);
				if(tempStr.contains("'")){
					stmt.setString(1, tempStr.substring(1, tempStr.length()-1));
				}
				else{
					stmt.setInt(1, Integer.valueOf(tempStr));
				}
//				System.out.println("Delete Col Value:" + k.substring(k.indexOf("=")+1));
			} else {
				ArrayList<String> cols = psp.getQueryCols();
				String tempStr = keys[0].substring(keys[0].indexOf("=")+1);
				if(tempStr.contains("'")){
					stmt.setString(cols.indexOf(keys[0].substring(0, keys[0].indexOf("=")))+1, tempStr.substring(1, tempStr.length()-1));
				}
				else{
					stmt.setInt(cols.indexOf(keys[0].substring(0, keys[0].indexOf("=")))+1, Integer.valueOf(tempStr));
				}
				for(int i = 1; i != keys.length; ++i){
					tempStr = keys[i].substring(keys[i].indexOf("=")+1);
					if(tempStr.contains("'")){
    					stmt.setString(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, tempStr.substring(1, tempStr.length()-1));
    				}
    				else{
    					stmt.setInt(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, Integer.valueOf(tempStr));
    				}
				}
//				ArrayList<String> cols = psp.getDeleteCols();
//				stmt.setObject(cols.indexOf(keys[0].substring(0, keys[0].indexOf("=")))+1, 
//						"'" + keys[0].substring(keys[0].indexOf("=")+1) + "'");
//				for(int i = 1; i != keys.length; ++i){
////					stmt.setInt(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, 
////							Integer.valueOf(keys[i].substring(keys[i].indexOf("=")+1)));
//					stmt.setObject(cols.indexOf(keys[i].substring(0, keys[i].indexOf("=")))+1, 
//							"'" + keys[i].substring(keys[i].indexOf("=")+1) + "'");
//				}
			}
    		stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStmt(stmt);
			closeConnection(con);
		}
	}

	@Override
	public void deleteAll(Collection<String> keys) {
        for(Object key : keys){
        	delete((String)key);
        }
	}

	@Override
	public HazelcastObject load(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, HazelcastObject> loadAll(Collection<String> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> loadAllKeys() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void closeConnection(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void closeStmt(Statement stmt) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void closeStmt(PreparedStatement stmt) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void closeResultSet(ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
