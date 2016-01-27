package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.easycache.jdbc.DBHelper;
import org.easycache.jdbc.IMDGHelper;
import org.easycache.load.LoadData;

public class MicroBenchMarkTest {
	
	private static Random rand = new Random();
	private static boolean flag = true;
	public static void pkQuery(){
		String sql = "SELECT * FROM item WHERE i_id = ?";
		Connection conn = null;
		if (flag) {
			conn = IMDGHelper.getConnection();
		}
		else {
			conn = DBHelper.getConnection();
		}
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			ResultSet rs = null;
			Random random = new Random(System.currentTimeMillis());
			int count = 1000;
			int range = 100000;
			long start = System.currentTimeMillis();
			for (int i = 0; i < count; i++) {
				pst.setInt(1, random.nextInt(range) + 1);
				rs = pst.executeQuery();
			}
			long end = System.currentTimeMillis();
			System.out.println( "pkQuery time : " + (end - start) + " ms");
			rs.close();
			pst.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void joinQuery(){
		String sql = "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND i_id = ?";
		Connection conn = null;
		if (flag) {
			conn = IMDGHelper.getConnection();
		}
		else {
			conn = DBHelper.getConnection();
		}
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			ResultSet rs = null;
			Random random = new Random(System.currentTimeMillis());
			int count = 1000;
			int range = 1000;
			long start = System.currentTimeMillis();
			for (int i = 0; i < count; i++) {
				pst.setInt(1, random.nextInt(range) + 1);
				rs = pst.executeQuery();
			}
			long end = System.currentTimeMillis();
			System.out.println( "joinQuery time : " + (end - start) + " ms");
			rs.close();
			pst.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void valueQuery(){
		String sql = "SELECT * FROM order_line, item WHERE ol_o_id = ? AND ol_i_id = i_id";
		Connection conn = null;
		if (flag) {
			conn = IMDGHelper.getConnection();
		}
		else {
			conn = DBHelper.getConnection();
		}
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			ResultSet rs = null;
			Random random = new Random(System.currentTimeMillis());
			int count = 1000;
			int range = 10000;
			long start = System.currentTimeMillis();
			for (int i = 0; i < count; i++) {
				pst.setInt(1, random.nextInt(range) + 1);
				rs = pst.executeQuery();
			}
			long end = System.currentTimeMillis();
			System.out.println( "valueQuery time : " + (end - start) + " ms");
			rs.close();
			pst.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void insert(){
		String sql = "INSERT into order_line (ol_id, ol_o_id, ol_i_id, ol_qty, ol_discount, ol_comments) VALUES (?, ?, ?, ?, ?, ?)";
		Connection conn = null;
		if (flag) {
			conn = IMDGHelper.getConnection();
		}
		else {
			conn = DBHelper.getConnection();
		}
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			Random random = new Random(System.currentTimeMillis());
			int count = 1000;
			int range = 100000;
			long start = System.currentTimeMillis();
			for (int i = 0; i < count; i++) {
				pst.setInt(1, random.nextInt(range) + 1);
				pst.setInt(2, random.nextInt(range) + 1);
				pst.setInt(3, random.nextInt(range) + 1);
				pst.setInt(4, random.nextInt(range) + 1);
				pst.setDouble(5,  (double) getRandomInt(0, 30) / 100);
				pst.setString(6, getRandomAString(20, 100));
				pst.executeUpdate();
			}
//			pst.executeBatch();
//			pst.clearBatch();
			long end = System.currentTimeMillis();
			System.out.println( "INSERT time : " + (end - start) + " ms");
			pst.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void insertBatch(){
		String sql = "INSERT into order_line (ol_id, ol_o_id, ol_i_id, ol_qty, ol_discount, ol_comments) VALUES (?, ?, ?, ?, ?, ?)";
		Connection conn = null;
		if (flag) {
			conn = IMDGHelper.getConnection();
		}
		else {
			conn = DBHelper.getConnection();
		}
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			Random random = new Random(System.currentTimeMillis());
			int count = 1000;
			int range = 10000;
			long start = System.currentTimeMillis();
			for (int i = 0; i < count; i++) {
				pst.setInt(1, random.nextInt(range) + 2000);
				pst.setInt(2, random.nextInt(range) + 2000);
				pst.setInt(3, random.nextInt(range) + 2000);
				pst.setInt(4, random.nextInt(range) + 2000);
				pst.setDouble(5,  (double) getRandomInt(0, 30) / 100);
				pst.setString(6, getRandomAString(20, 100));
				pst.addBatch();
			}
			pst.executeBatch();
			pst.clearBatch();
			long end = System.currentTimeMillis();
			System.out.println( "INSERT time : " + (end - start));
			pst.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void update(){
		String sql = "UPDATE item SET i_stock = ? WHERE i_id = ?";
		Connection conn = null;
		if (flag) {
			conn = IMDGHelper.getConnection();
		}
		else {
			conn = DBHelper.getConnection();
		}
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			Random random = new Random(System.currentTimeMillis());
			int count = 1000;
			int range = 100000;
			long start = System.currentTimeMillis();
			for (int i = 0; i < count; i++) {
				pst.setInt(1, random.nextInt(range) + 1);
				pst.setInt(2, random.nextInt(range) + 1);
				pst.executeUpdate();
			}
			long end = System.currentTimeMillis();
			System.out.println( "update time : " + (end - start) + " ms");
			pst.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static String getRandomAString(int min, int max) {
		String newstring = new String();
		int i;
		final char[] chars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
				'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C',
				'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
				'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#', '$', '%', '^', '&', '*', '(',
				')', '_', '-', '=', '+', '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/',
				'~', ' ' }; 
		int strlen = rand.nextInt(max-min) + min;
		for (i = 0; i < strlen; i++) {
			char c = chars[rand.nextInt(chars.length)];
			newstring = newstring.concat(String.valueOf(c));
		}
		return newstring;
	}	
	
	public static int getRandomInt(int lower, int upper) {
		return rand.nextInt(upper + 1 - lower) + lower;
	}
	
	public static void IMDGTest(){
		flag = true;
		new LoadData();
//		pkQuery();
		joinQuery();
//		insert();
//		insertBatch();
//		valueQuery();
//		update();
//		update();
		System.out.println("IMDGTest done!");
	}
	
	public static void DBTest(){
		flag = false;
//		pkQuery();
//		joinQuery();
//		insert();
//		insertBatch();
//		valueQuery();
		update();
		
		System.out.println("DBTest done");
	}
	
	public static void main(String[] args){
		IMDGTest();
//		DBTest();
	}
}
