package org.easycache.jdbc;

import java.sql.Connection;
import com.easycache.sqlclient.jdbc.IMDGConnection;

public class IMDGHelper {
	private static Connection con = null;

	public static Connection getConnection() {
		con = DBHelper.getConnection();
		if (con != null) {
			return new IMDGConnection(con);
		} else {
			System.out.println("get connection failed.");
			return null;
		}
	}


	public static void closeConnection() {
		DBHelper.closeConnection();
	}

}
