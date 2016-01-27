package com.easycache.sqlclient.jdbc;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.easycache.sqlclient.load.ConfigParser;
import com.easycache.sqlclient.load.Loader;
import com.hazelcast.easycache.utility.ConnectionPool;

public class IMDGDriver implements java.sql.Driver {
	public static boolean loadflag = false;
	static {
		try {
			java.sql.DriverManager.registerDriver(new IMDGDriver());
		} catch (Exception e) {
			throw new RuntimeException("Can't register driver!");
		}
	}

	
	public IMDGDriver(){} 
	
	public Connection connect(String url, Properties info) throws SQLException {
		//jiang yong 2015-1-9
		//for DriverManager.getconnection return wrong connection
		if(!url.contains("jdbc:imdg")){
//			System.out.println("warnnig: unreasonable url:" + url);
			return null;
		}
		//done
		if(!loadflag) {
			synchronized (IMDGDriver.class) {
				if(!loadflag) {
					Loader loader = new Loader();
					long start = System.currentTimeMillis();
					loader.loadData();
					long end = System.currentTimeMillis();
					DecimalFormat fnum = new DecimalFormat("##0.00");    
					String time=fnum.format((float)(end-start)/1000);       
					System.out.println("data loaded done in " + time + " s");
				}
				loadflag = true;
			}
		}
		return new IMDGConnection(ConnectionPool.getConnection());
	}

	public boolean acceptsURL(String url) throws SQLException {
		if (ConnectionPool.getConnection() != null)
			return true;
		else
			return false;
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		return null;
	}

	public int getMajorVersion() {
		return 1;
	}

	public int getMinorVersion() {
		return 1;
	}

	public boolean jdbcCompliant() {
		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
	
}