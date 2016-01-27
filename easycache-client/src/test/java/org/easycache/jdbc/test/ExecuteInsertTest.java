package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.easycache.jdbc.DBHelper;
import org.easycache.load.LoadData;

import com.easycache.sqlclient.jdbc.IMDGConnection;

public class ExecuteInsertTest {
	
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
	
	public static void sqlExecuteInsert(){
		String sql = "INSERT into shopping_cart_line (scl_sc_id, scl_qty, scl_i_id) VALUES (?,?,?)";
		Connection conn= getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			int scl_sc_id =  1;
			int scl_qty = 2;
			int scl_i_id = 3;
			pst.setInt(1, scl_sc_id);
			pst.setInt(2, scl_qty);
			pst.setInt(3, scl_i_id);
			pst.executeUpdate();
			System.out.println("insert done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}
	
	public static void sqlExecuteInsert2(){
		String sql = "Insert shopping_cart (sc_id, sc_time) values (?, ?)";
		Connection conn= getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			int sc_id = 2;
			java.sql.Timestamp sc_time = new java.sql.Timestamp(System.currentTimeMillis());
			pst.setInt(1, sc_id);
			pst.setTimestamp(2, sc_time);
			pst.executeUpdate();
			System.out.println("insert done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}
	
	public static void sqlExecuteInsert_autoKey(){
		String sql = "Insert shopping_cart (sc_time) values (?)";
		Connection conn= getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
//			int sc_id = 2;
			java.sql.Timestamp sc_time = new java.sql.Timestamp(System.currentTimeMillis());
//			pst.setInt(1, sc_id);
			pst.setTimestamp(1, sc_time);
			pst.executeUpdate();
			System.out.println("insert done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeConnection();
	}
	
	public static void test(){
		new LoadData();
//		sqlExecuteInsert_autoKey();
//		sqlExecuteInsert_autoKey();
		sqlExecuteInsert();
	}
	
	public static void main(String [] args){
		test();
	}
}