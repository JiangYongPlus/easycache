package com.easycache.sqlclient.log;

public class IMDGPreparedStatementLog {
	public static boolean flag = false;
	
	public static void print(String str) {
		if (flag) {
			System.out.println("IMDGPreparedStatement log:  " + str);
		}
	}
	
	public static void funcLog(String str) {
		if (flag) {
			System.out.println("IMDGPreparedStatement function: " + str + " is executed!");
		}   
	}
	
	public static void showSql(String sql) {
		if (flag) {
			System.out.println("---IMDGPreparedStatement sql is: " + sql + "----");
		} 
	}

}
