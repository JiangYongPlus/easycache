package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.easycache.jdbc.DBHelper;

import com.easycache.sqlclient.jdbc.IMDGConnection;
import com.easycache.sqlclient.load.Loader;
import com.hazelcast.easycache.utility.ConnectionPool;

public class PrimaryKeyTest {
	private static Connection con = null;
	
	public static Connection getConnection() {
		con = ConnectionPool.getConnection();
		if(con != null){
			return new IMDGConnection(con);
		}
		else{
			System.out.println("get connection failed.");
			return null;
		}
	}
	
	public static void closeConnection(){
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void loadData(){
		Loader loader = new Loader();
		long start = System.currentTimeMillis();
		loader.loadData();
		long end = System.currentTimeMillis();
		System.out.println( "loadData Time : " + (end - start));
	}
	
	public static void sqlExecuteUpdateForStringKey(){
		String sql = "INSERT INTO STRINGKEY(STRING_ID,STRING_INFO,STRING_VAL) VALUES (?, ?, ?)";
		Connection conn= getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i = 0; i < 10; i++) {
				String STRING_ID = "testId00" + i;
				String STRING_INFO = "testInfo00" + i;
				int STRING_VAL = 1;
				pst.setString(1, STRING_ID);
				pst.setString(2, STRING_INFO);
				pst.setInt(3, STRING_VAL);
				pst.executeUpdate();
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}
	
	public static void sqlExecuteUpdateForUnionKey(){
		String sql = "INSERT INTO UNIONKEY(STRING_ID, INT_ID, STRING_INFO, STRING_VAL) VALUES (?, ?, ?, ?)";
		Connection conn= getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i = 0; i < 10; i++) {
				String STRING_ID = "testId00" + i;
				int INT_ID = 9;
				String STRING_INFO = "testInfo00" + i;
				int STRING_VAL = 1;
				pst.setString(1, STRING_ID);
				pst.setInt(2, INT_ID);
				pst.setString(3, STRING_INFO);
				pst.setInt(4, STRING_VAL);
				pst.executeUpdate();
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}
	
	public static void test(){
		loadData();
		sqlExecuteUpdateForStringKey();
//		sqlExecuteUpdateForUnionKey();
	}
	
	public static void main(String [] args){
		test();
	}
}
