package org.easycache.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBHelper {
	public static String driver;
	public static String url;
	public static String user;
	public static String password;
	private static Connection con = null;
	private static String dbType = "mysql";
	static{
		
		if (dbType.equals("mysql")) {
			driver = "com.mysql.jdbc.Driver";
			url = "jdbc:mysql://127.0.0.1:3306/bench4q";
			user = "root";
			password = "root";
		}
		else if (dbType.equals("shentong")) {
			driver = "com.oscar.Driver";
			url = "jdbc:oscar://localhost:2015/IMDG_New";
			user = "SYSDBA";
			password = "szoscar55";
//			driver = "com.oscar.Driver";
//			url = "jdbc:oscar://10.126.3.13:2003/dzgw_lb01";
//			user = "dzgw01";
//			password = "dzgw";
		}
		else {
			try {
				throw new Exception("error dbType: " + dbType);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static Connection getConnection(){
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(url, user, password);
			return con;
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	public static void closeConnection() {
		try {
			con.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}
}
