/*
 * jiang yong 2015-06-11
 * get Connection for persistence, use tomcat jdbc connection pool
 */
package com.hazelcast.easycache.utility;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

public class ConnectionPool {
	
	private static String connectionPoolFile = "connectionPool.properties";
	private static String url = null;
	private static String driverClassName = null;
	private static String userName = null;
	private static String password = null;
	private static int maxIdle = 100;
	private static int maxWait = 30000;
	private static int maxActive = 100;
	private static boolean isPropInited= false;
	private static DataSource datasource = new DataSource();
	
	public static void init() {
		try {
			Properties prop = new Properties();  
	    	InputStream in = ConnectionPool.class.getResourceAsStream(connectionPoolFile);
	    	if (in != null) {
	    		prop.load(in);
	    		url = prop.getProperty("url", null);
	    		driverClassName = prop.getProperty("driverClassName", null);
	    		userName = prop.getProperty("userName", null);
	    		password = prop.getProperty("password", null);
	    		maxIdle = Integer.parseInt(prop.getProperty("maxIdle", "100"));
	    		maxWait = Integer.parseInt(prop.getProperty("maxWait", "30000"));
	    		maxActive = Integer.parseInt(prop.getProperty("maxActive", "100"));
	    	} else {
				throw new IOException("error :can't find connectionPool.properties");
	    	}
		} catch (IOException e) {
			e.printStackTrace();
		}
		PoolProperties p = new PoolProperties();
		p.setUrl(url);
		p.setDriverClassName(driverClassName);
		p.setUsername(userName);
		p.setPassword(password);
		p.setMaxActive(maxActive);
		p.setMaxWait(maxWait);
		p.setMaxIdle(maxIdle);
		datasource.setPoolProperties(p);
		isPropInited = true;
	}
	
	public static Connection getConnection() {
		if (!isPropInited) {
			init();
		}
		Connection con = null;
		try {
			con = datasource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (con == null) {
			try {
				throw new Exception("getConnection failed, connection is null!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return con;
	}
	
	public static void main(String[] args) {
		Connection con1 = ConnectionPool.getConnection();
		Connection con2 = ConnectionPool.getConnection();
		String sql = "select * from item limit 1";
		try {
			con1.createStatement().executeQuery(sql);
			con1.close();
			con2.createStatement().executeQuery(sql);
			con2.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
