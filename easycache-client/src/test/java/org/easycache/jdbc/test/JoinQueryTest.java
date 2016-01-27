package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Random;

import org.easycache.jdbc.DBHelper;
import org.easycache.jdbc.IMDGHelper;
import org.easycache.load.LoadData;
import org.easycache.utility.Print;

public class JoinQueryTest {
private static Connection con = null;
	
	public static void joinQuery1() {
		String sql = "select * from item, author where i_id = 1 and item.i_a_id = author.a_id";
		try {
			PreparedStatement pst = con.prepareStatement(sql);
			ResultSet rst = pst.executeQuery();
			while (rst.next()) {
				Print.show("" + rst.getInt(1));
				Print.show(rst.getString(2));
			}
			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void executeQuery2() {
		String sql = "select * from item where i_id = ?";
		try {
			PreparedStatement pst = con.prepareStatement(sql);
			pst.setInt(1, 1);
			ResultSet rst = pst.executeQuery();
			while (rst.next()) {
				Print.show(rst.getString(2));
			}
			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void executeQuery3() {
//		String sql = "SELECT * FROM author, item WHERE author.a_lname LIKE ? AND item.i_a_id = author.a_id AND rownum < 51 ORDER BY item.i_title";
		String sql = "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND item.i_subject = ? AND rownum < 51 ORDER BY item.i_title";
		try {
			PreparedStatement pst = con.prepareStatement(sql);
			pst.setString(1, "ARTS");
			ResultSet rst = pst.executeQuery();
//			while (rst.next()) {
//				Print.show(rst.getString(1));
//			}
			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void executeQuery4() {
		String sql = "SELECT * FROM author, item WHERE author.a_lname LIKE '%' AND item.i_a_id = author.a_id AND rownum < 51 ORDER BY item.i_title";
		try {
			Statement stmt = con.createStatement();
			ResultSet rst = stmt.executeQuery(sql);
//			while (rst.next()) {
//				Print.show(rst.getString(1));
//			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void executeQuery5(int id) {
		String sql = "SELECT co_id FROM address, country WHERE addr_id = ? AND addr_co_id = co_id";
		try {
			PreparedStatement pst = con.prepareStatement(sql);
			pst.setInt(1, id);
			ResultSet rst = pst.executeQuery();
//			while (rst.next()) {
//				Print.show(rst.getString(1));
//			}
			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void executeQuery6(int id) {
		String sql = "SELECT co_id FROM address, country WHERE addr_id = " + id + " AND addr_co_id = co_id";
//		System.out.println(sql);
		try {
			Statement stmt = con.createStatement();
			ResultSet rst = stmt.executeQuery(sql);
//			while (rst.next()) {
//				Print.show(rst.getString(1));
//			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void dbTest() {
		con = DBHelper.getConnection();
		long start = System.currentTimeMillis();
		Random random = new Random(System.currentTimeMillis());
		for (int i = 0; i < 10000; i++) {
			executeQuery3();
		}
		long end = System.currentTimeMillis();
		DecimalFormat fnum = new DecimalFormat("##0.00");
		String time = fnum.format((float) (end - start) / 1000);
		System.out.println("execute time: " + time + " s");
		DBHelper.closeConnection();
	}
	
	public static void IMDGTest1() {
		con = IMDGHelper.getConnection();
		new LoadData();
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			executeQuery3();
		}
		long end = System.currentTimeMillis();
		DecimalFormat fnum = new DecimalFormat("##0.00");
		String time = fnum.format((float) (end - start) / 1000);
		System.out.println("execute time: " + time + " s");
		IMDGHelper.closeConnection();
	}
	
	public static void IMDGTest2() {
		con = IMDGHelper.getConnection();
		new LoadData();
		joinQuery1();
		IMDGHelper.closeConnection();
	}
	
	public static void main(String [] args) {
//		IMDGTest();
//		dbTest();
		IMDGTest2();
	}
}
