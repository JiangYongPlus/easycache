package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.easycache.jdbc.DBHelper;
import org.easycache.load.LoadData;

import com.easycache.sqlclient.jdbc.IMDGConnection;
import com.easycache.sqlclient.load.Loader;

public class ExecuteUpdateTest {
	private static Connection con = null;
	
	public static Connection getConnection() {
		con = DBHelper.getConnection();
		if(con != null){
			return new IMDGConnection(con);
		}
		else{
			System.out.println("get connection failed.");
			return null;
		}
	}
	
	public static void closeConnection(){
		DBHelper.closeConnection();
	}
	
	public static void loadData(){
		Loader loader = new Loader();
		long start = System.currentTimeMillis();
		loader.loadData();
		long end = System.currentTimeMillis();
		System.out.println( "loadData Time : " + (end - start));
	}
	
	public static void sqlExecuteUpdate(){
		String sql = "INSERT INTO STRINGKEY(STRING_ID,STRING_INFO,STRING_VAL) VALUES (?, ?, ?)";
		Connection conn= getConnection();
		try {
			for(int i =0; i < 5; i++){
				PreparedStatement pst = conn.prepareStatement(sql);
				String STRING_ID = "testId000" + (i+1);
				String STRING_INFO = "testInfo000" + (i+1);
				int STRING_VAL = i + 1;
				pst.setString(1, STRING_ID);
				pst.setString(2, STRING_INFO);
				pst.setInt(3, STRING_VAL);
				pst.executeUpdate();
			}
			System.out.println("update done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}

	public static void sqlExecuteUpdate2(){
		String sql = "UPDATE shopping_cart SET sc_time = ? WHERE sc_id = ?";
		Connection conn= getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			int sc_id = 2;
			java.sql.Timestamp sc_time = new java.sql.Timestamp(System.currentTimeMillis());
			pst.setTimestamp(1, sc_time);
			pst.setInt(2, sc_id);
			pst.executeUpdate();
			System.out.println("update done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}
	
	public static void sqlExecuteSelect(){
//		String sql = "SELECT * From STRINGKEY where STRING_ID like 'testId000%' ";
		String sql = "SELECT * From STRINGKEY WHERE STRING_ID = ?";
		Connection conn= getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			pst.setString(1, "testId0002");
			ResultSet rst = pst.executeQuery();
			while(rst.next()){
				System.out.println(rst.getString(1) + " " + rst.getString(2) + " " + rst.getString(3));
			}
			System.out.println("query done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}
	
	public static void test(){
		new LoadData();
		sqlExecuteUpdate2();
	}
	
	public static void main(String [] args){
		test();
	}
}
