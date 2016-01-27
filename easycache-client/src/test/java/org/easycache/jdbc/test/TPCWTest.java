package org.easycache.jdbc.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

import org.easycache.jdbc.DBHelper;
import org.easycache.jdbc.IMDGHelper;
import org.easycache.load.LoadData;

public class TPCWTest {
	public void sqlExecuteQuerySupport() {
		try {
			String sql = "select * from item,author where item.i_a_id = author.a_id and i_id = ?";
			Connection connImdg = IMDGHelper.getConnection();
			PreparedStatement pstImdg = connImdg.prepareStatement(sql);
			pstImdg.setInt(1, 45);
			ResultSet rsSetImdg = pstImdg.executeQuery();
			while (rsSetImdg.next()) {
				System.out.println(rsSetImdg.getString(1));
				System.out.println(rsSetImdg.getString(2));
				System.out.println(rsSetImdg.getString(3));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void sqlExecuteQueryNotSupport() {
		try {
			String sql ="SELECT i_id, i_title, a_fname, a_lname FROM item, author WHERE item.i_a_id = author.a_id AND item.i_subject = ? ORDER BY item.i_pub_date DESC,item.i_title LIMIT 3";
//			String sql= "SELECT i_id, i_title, a_fname, a_lname "
//				+ "FROM item, author, order_line "
//				+ "WHERE item.i_id = order_line.ol_i_id "
//				+ "AND item.i_a_id = author.a_id "
//				+ "AND order_line.ol_o_id > (SELECT MAX(o_id)-3333 FROM orders)"
//				+ "AND item.i_subject = ? "
//				+ "GROUP BY i_id, i_title, a_fname, a_lname "
//				+ "ORDER BY SUM(ol_qty) DESC " + "LIMIT 50";
//			String sql = "SELECT addr_id FROM address "
//				+ "WHERE addr_street1 = ? " + "AND addr_street2 = ? "
//				+ "AND addr_city = ? " + "AND addr_state = ? "
//				+ "AND addr_zip = ? " + "AND addr_co_id = ?";
			Connection connImdg = IMDGHelper.getConnection();
			PreparedStatement pstImdg = connImdg.prepareStatement(sql);
			pstImdg.setString(1, "arts");
			ResultSet rsSetImdg = pstImdg.executeQuery();
			while (rsSetImdg.next()) {
				System.out.println(rsSetImdg.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sqlExecuteUpdate() {
		try {
			String sql = "update shopping_cart set sc_time = ? where sc_id = ?";
			Connection connImdg = IMDGHelper.getConnection();
			PreparedStatement pstImdg = connImdg.prepareStatement(sql);
			Timestamp now = new Timestamp(
					new GregorianCalendar().getTimeInMillis());
			pstImdg.setTimestamp(1, now);
			pstImdg.setInt(2,1);
			pstImdg.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void sqlExecuteQueryOracleDate() {
		try {
			String sql = "select * from CUSTOMER where c_uname = ?";
			Connection conn = IMDGHelper.getConnection();
			PreparedStatement pst = conn.prepareStatement(sql);
			pst.setString(1, "OG");
			ResultSet rsSet = pst.executeQuery();
			while (rsSet.next()) {
				System.out.println(rsSet.getInt(1));
				System.out.println(rsSet.getDate(9));
				System.out.println(rsSet.getDate(10));
				System.out.println(rsSet.getTimestamp(11));
				System.out.println(rsSet.getTimestamp(12));

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sqlExecuteQuery3() throws ClassNotFoundException, SQLException {
		// "SELECT J.i_id,J.i_thumbnail from item I, item J where (I.i_related1 = J.i_id or I.i_related2 = J.i_id or I.i_related3 = J.i_id or I.i_related4 = J.i_id or I.i_related5 = J.i_id) and I.i_id = ?";
		String sql = "select j.i_id,j.i_thumbnail from item i, item j where (i.i_related1 = j.i_id or i.i_related2 = j.i_id or i.i_related3 = j.i_id or i.i_related4 = j.i_id or i.i_related5 = j.i_id) and i.i_id = 914";
		Connection connMysql = DBHelper.getConnection();
		PreparedStatement pstMysql = connMysql.prepareStatement(sql);
		ResultSet rsSetMysql = pstMysql.executeQuery();
	}
	
	public void test(){
		new LoadData();
		sqlExecuteQuerySupport();
	}
	
	public static void main(String [] args) {
		new TPCWTest().test();
	}
}
