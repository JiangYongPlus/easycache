package com.easycache.sqlclient.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class IMDGParametermetaData implements ParameterMetaData {

	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	
	public int getParameterCount() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public int isNullable(int param) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public boolean isSigned(int param) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	
	public int getPrecision(int param) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public int getScale(int param) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public int getParameterType(int param) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public String getParameterTypeName(int param) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public String getParameterClassName(int param) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public int getParameterMode(int param) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

}
