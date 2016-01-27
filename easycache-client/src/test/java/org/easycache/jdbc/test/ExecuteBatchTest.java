package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.easycache.jdbc.IMDGHelper;

public class ExecuteBatchTest {

	public static void executeInsert() {
		String sql = "INSERT INTO STRINGKEY(STRING_ID,STRING_INFO,STRING_VAL) VALUES (?, ?, ?)";
		Connection conn = IMDGHelper.getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			// long start = System.currentTimeMillis();
			// for (int i = 0; i < 10; i++) {
			// String STRING_ID = "testId000" + (i + 1);
			// String STRING_INFO = "testInfo000" + (i + 1);
			// int STRING_VAL = i + 1;
			// pst.setString(1, STRING_ID);
			// pst.setString(2, STRING_INFO);
			// pst.setInt(3, STRING_VAL);
			// pst.executeUpdate();
			// }
			// long end = System.currentTimeMillis();
			// System.out.println("update Time : " + (end - start));
			long start2 = System.currentTimeMillis();
			for (int i = 10; i < 100; i++) {
				String STRING_ID = "testId000" + (i + 1);
				String STRING_INFO = "testInfo000" + (i + 1);
				int STRING_VAL = i + 1;
				pst.setString(1, STRING_ID);
				pst.setString(2, STRING_INFO);
				pst.setInt(3, STRING_VAL);
				pst.executeUpdate();
			}
			long end2 = System.currentTimeMillis();
			System.out.println("update Time : " + (end2 - start2));
			System.out.println("insert done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		IMDGHelper.closeConnection();
	}

	public static void executeBatchInsert() {
		String sql = "INSERT INTO STRINGKEY(STRING_ID,STRING_INFO,STRING_VAL) VALUES (?, ?, ?)";
		Connection conn = IMDGHelper.getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			long start = System.currentTimeMillis();
			for (int i = 0; i < 10; i++) {
				String STRING_ID = "testId000" + (i + 1);
				String STRING_INFO = "testInfo000" + (i + 1);
				int STRING_VAL = i + 1;
				pst.setString(1, STRING_ID);
				pst.setString(2, STRING_INFO);
				pst.setInt(3, STRING_VAL);
				pst.addBatch();
				// if(i%10 == 0){
				// pst.executeBatch();
				// pst.clearBatch();
				// }
			}
			pst.executeBatch();
			pst.clearBatch();
			long end = System.currentTimeMillis();
			System.out.println("update Time : " + (end - start));
			long start2 = System.currentTimeMillis();
			for (int i = 100; i < 5100; i++) {
				String STRING_ID = "testId000" + (i + 1);
				String STRING_INFO = "testInfo000" + (i + 1);
				int STRING_VAL = i + 1;
				pst.setString(1, STRING_ID);
				pst.setString(2, STRING_INFO);
				pst.setInt(3, STRING_VAL);
				pst.addBatch();
				// if(i%10 == 0){
				// pst.executeBatch();
				// pst.clearBatch();
				// }
			}
			pst.executeBatch();
			pst.clearBatch();
			long end2 = System.currentTimeMillis();
			System.out.println("update Time : " + (end2 - start2));
			System.out.println("insert done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		IMDGHelper.closeConnection();
	}

	public static void executeBatchInsert2() {
		String sql = "INSERT INTO STRINGKEY(STRING_ID,STRING_INFO,STRING_VAL) VALUES (?, ?, ?)";
		Connection conn = IMDGHelper.getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i = 0; i < 37; i++) {
				String STRING_ID = "testId000" + (i + 1);
				String STRING_INFO = "testInfo000" + (i + 1);
				int STRING_VAL = i + 1;
				pst.setString(1, STRING_ID);
				pst.setString(2, STRING_INFO);
				pst.setInt(3, STRING_VAL);
				pst.addBatch();
				// if(i%10 == 0){
				// pst.executeBatch();
				// pst.clearBatch();
				// }
			}
			pst.executeBatch();
			pst.clearBatch();
			System.out.println("insert done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		IMDGHelper.closeConnection();
	}

	public static void executeBatchDelete() {
		String sql = "DELETE FROM STRINGKEY WHERE STRING_ID = ?";
		Connection conn = IMDGHelper.getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i = 0; i < 37; i++) {
				String STRING_ID = "testId000" + (i + 1);
				pst.setString(1, STRING_ID);
				pst.addBatch();
			}
			pst.executeBatch();
			pst.clearBatch();
			System.out.println("delete done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		IMDGHelper.closeConnection();
	}

	public static void executeDelete() {
		String sql = "DELETE FROM STRINGKEY WHERE STRING_ID = ?";
		Connection conn = IMDGHelper.getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i = 0; i < 50; i++) {
				String STRING_ID = "testId000" + (i + 1);
				pst.setString(1, STRING_ID);
				pst.executeUpdate();
			}
			System.out.println("delete done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		IMDGHelper.closeConnection();
	}

	public static void executeBatchUpdate() {
		String sql = "update stringkey set string_val = ? WHERE STRING_ID = ?";
		Connection conn = IMDGHelper.getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i = 0; i < 37; i++) {
				String STRING_ID = "testId000" + (i + 1);
				int STRING_VAL = i + 2;
				pst.setInt(1, STRING_VAL);
				pst.setString(2, STRING_ID);
				pst.addBatch();
			}
			pst.executeBatch();
			pst.clearBatch();
			System.out.println("update done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		IMDGHelper.closeConnection();
	}

	public static void executeUpdate() {
		String sql = "update stringkey set string_val = ? WHERE STRING_ID = ?";
		Connection conn = IMDGHelper.getConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i = 0; i < 37; i++) {
				String STRING_ID = "testId000" + (i + 1);
				int STRING_VAL = i + 1;
				pst.setInt(1, STRING_VAL);
				pst.setString(2, STRING_ID);
				pst.executeUpdate();
			}
			System.out.println("update done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		IMDGHelper.closeConnection();
	}

	public static void test() {
//		 IMDGHelper.loadData();
		long start = System.currentTimeMillis();
		// executeInsert();
		executeBatchInsert();
		// executeBatchDelete();
		// executeDelete();
		// executeBatchUpdate();
		// executeUpdate();
		long end = System.currentTimeMillis();
		System.out.println("update Time : " + (end - start));
	}

	public static void main(String[] args) {
		test();
	}
}
