package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Random;

import org.easycache.jdbc.DBHelper;
import org.easycache.jdbc.IMDGHelper;
import org.easycache.load.LoadData;

public class StatementTest {
	
	private static Connection con = null;
	private static Statement stmt = null; 
	
	public static void stmtTest1() {
		int id;
		Random random = new Random(System.currentTimeMillis());
		id = random.nextInt(20000) + 1;
		String sql = "SELECT co_id FROM address, country WHERE addr_id = " + id + " AND addr_co_id = co_id";
		try {
			stmt = con.createStatement();
			for (int i = 0; i < 100000; i++) {
				stmt.executeQuery(sql);
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void stmtTest2() {
		
		String sql = "SELECT * FROM author, item WHERE author.a_lname LIKE '%' AND item.i_a_id = author.a_id AND rownum < 51 ORDER BY item.i_title";
		try {
			stmt = con.createStatement();
			for (int i = 0; i < 1000; i++) {
				stmt.executeQuery(sql);
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void stmtTest3() {

		String sql = "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND item.i_subject = 'ARTS' AND rownum < 51 ORDER BY item.i_title";
		try {
			stmt = con.createStatement();
			for (int i = 0; i < 10000; i++) {
				stmt.executeQuery(sql);
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	
	public static void IMDGTest() {
		
		con = IMDGHelper.getConnection();
		new LoadData();
		long start = System.currentTimeMillis();
		stmtTest1();
		long end = System.currentTimeMillis();
		DecimalFormat fnum = new DecimalFormat("##0.00");
		String time = fnum.format((float) (end - start) / 1000);
		System.out.println("execute time: " + time + " s");
		IMDGHelper.closeConnection();
	}
	
	public static void DBTest() {
		
		con = DBHelper.getConnection();
		long start = System.currentTimeMillis();
		stmtTest1();
		long end = System.currentTimeMillis();
		DecimalFormat fnum = new DecimalFormat("##0.00");
		String time = fnum.format((float) (end - start) / 1000);
		System.out.println("execute time: " + time + " s");
		DBHelper.closeConnection();
	}
	
	public static void main(String [] args) {
		IMDGTest();
//		DBTest();
	}
}
