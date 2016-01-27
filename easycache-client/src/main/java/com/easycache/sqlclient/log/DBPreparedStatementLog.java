package com.easycache.sqlclient.log;

public class DBPreparedStatementLog {
	public static boolean flag = false;
	
	public static void print(String str) {
		if (flag) {
			System.out.println("DBPreparedStatementLog log:  " + str);
		}
	}
	
	public static void funcLog(String str) {
		if (flag) {
			System.out.println("DBPreparedStatementLog function: " + str + " is executed!");
		}   
	}
	
	public static void showSql(String sql) {
		if (flag) {
			System.out.println("---DBPreparedStatementLog sql is: " + sql + "----");
		} 
	}
}
