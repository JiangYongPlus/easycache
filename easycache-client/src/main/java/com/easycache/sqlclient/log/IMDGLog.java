package com.easycache.sqlclient.log;

public class IMDGLog {
	
	public static boolean flag = false;
	
	public static void print(String str) {
		if (flag) {
			System.out.println("IMDG log:  " + str);
		}
	}
	
	public static void funcLog(String str) {
		if (flag) {
			System.out.println("function: " + str + " is executed!");
		}   
	}
	
	public static void rstFuncLog(String str) {
		if (flag) {
			System.out.println("---IMDGResultSet function: " + str + " is executed!---");
		} 	
	}
	
	public static void stmtFuncLog(String str) {
		if (flag) {
			System.out.println("---Statement function: " + str + " is executed!---");
		} 
	}
	public static void showPstSql(String sql) {
		if (flag) {
			System.out.println("---preparedstatement sql is: " + sql + "----");
		} 
	}
	
	public static void showStmtSql(String sql) {
		if (flag) {
			System.out.println("--statement sql is: " + sql + "----");
		} 
	}
}
