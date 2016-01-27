package com.easycache.sqlclient.log;

import java.sql.SQLException;

public class DBQueryPSTLog {
	
	public static boolean flag = false;
	
	public static void print(String str) {
		if (flag) {
			System.out.println("DBQueryPreparedStatement log:  " + str);
		}
	}
	
	public static void funcLog(String str) {
		if (flag) {
			System.out.println("DBQueryPreparedStatement function: " + str + " is executed!");
		}   
	}
	
	public static void funcNotImplemented(String str) throws SQLException {
		throw new SQLException("DBQueryPreparedStatement function: " + str + " is not Implemented!");
	}
}
