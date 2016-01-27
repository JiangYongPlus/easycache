/*
 * jiang yong 2015.03
 * DBPreparedStatement.java
 * if the sql is not supported, create a DBPreparedStatement
 */
package com.easycache.sqlclient.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.easycache.sqlclient.log.DBPreparedStatementLog;

public class DBPreparedStatement implements java.sql.PreparedStatement{
	
	protected PreparedStatement pst = null;
	
	public DBPreparedStatement(PreparedStatement preparedStatement) {
		pst = preparedStatement;
	}
 
	
	public ResultSet executeQuery(String sql) throws SQLException {
		DBPreparedStatementLog.funcLog("executeQuery(String sql)");
		return pst.executeQuery(sql);
	}

	
	public int executeUpdate(String sql) throws SQLException {
		
		return pst.executeUpdate();
	}

	
	public void close() throws SQLException {
		
		pst.close();
	}

	
	public int getMaxFieldSize() throws SQLException {
		DBPreparedStatementLog.funcLog("getMaxFieldSize()");
		return pst.getMaxFieldSize();
	}

	
	public void setMaxFieldSize(int max) throws SQLException {
		
		pst.setMaxFieldSize(max);
	}

	
	public int getMaxRows() throws SQLException {
		DBPreparedStatementLog.funcLog("getMaxRows()");
		return pst.getMaxRows();
	}

	
	public void setMaxRows(int max) throws SQLException {
		
		pst.setMaxRows(max);
	}

	
	public void setEscapeProcessing(boolean enable) throws SQLException {
		
		pst.setEscapeProcessing(enable);
	}

	
	public int getQueryTimeout() throws SQLException {
		DBPreparedStatementLog.funcLog("getQueryTimeout()");
		return pst.getQueryTimeout();
	}

	
	public void setQueryTimeout(int seconds) throws SQLException {
		
		pst.setQueryTimeout(seconds);
	}

	
	public void cancel() throws SQLException {
		
		pst.cancel();
	}

	
	public SQLWarning getWarnings() throws SQLException {
		
		return pst.getWarnings();
	}

	
	public void clearWarnings() throws SQLException {
		
		pst.clearWarnings();
	}

	
	public void setCursorName(String name) throws SQLException {
		
		pst.setCursorName(name);
	}

	
	public boolean execute(String sql) throws SQLException {
		
		return pst.execute(sql);
	}

	
	public ResultSet getResultSet() throws SQLException {
		DBPreparedStatementLog.funcLog("getResultSet()");
		return pst.getResultSet();
	}

	
	public int getUpdateCount() throws SQLException {
		DBPreparedStatementLog.funcLog("getUpdateCount()");
		return pst.getUpdateCount();
	}

	
	public boolean getMoreResults() throws SQLException {
		DBPreparedStatementLog.funcLog("getMoreResults()");
		return pst.getMoreResults();
	}

	
	public void setFetchDirection(int direction) throws SQLException {
		
		pst.setFetchDirection(direction);
	}

	
	public int getFetchDirection() throws SQLException {
		DBPreparedStatementLog.funcLog("getFetchDirection()");
		return pst.getFetchDirection();
	}

	
	public void setFetchSize(int rows) throws SQLException {
		
		pst.setFetchSize(rows);
	}

	
	public int getFetchSize() throws SQLException {
		DBPreparedStatementLog.funcLog("getFetchSize()");
		return pst.getFetchSize();
	}

	
	public int getResultSetConcurrency() throws SQLException {
		
		return pst.getResultSetConcurrency();
	}

	
	public int getResultSetType() throws SQLException {
		
		return pst.getResultSetType();
	}

	
	public void addBatch(String sql) throws SQLException {
		
		pst.addBatch();
	}

	
	public void clearBatch() throws SQLException {
		
		pst.clearBatch();
	}

	
	public int[] executeBatch() throws SQLException {
		DBPreparedStatementLog.funcLog("executeBatch()");
		return pst.executeBatch();
	}

	
	public Connection getConnection() throws SQLException {
		DBPreparedStatementLog.funcLog("getConnection()");
		return pst.getConnection();
	}

	
	public boolean getMoreResults(int current) throws SQLException {
		DBPreparedStatementLog.funcLog("getMoreResults(int current)");
		return pst.getMoreResults(current);
	}

	
	public ResultSet getGeneratedKeys() throws SQLException {
		DBPreparedStatementLog.funcLog("getGeneratedKeys()");
		return pst.getGeneratedKeys();
	}

	
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		
		return pst.executeUpdate(sql, autoGeneratedKeys);
	}

	
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		
		return pst.executeUpdate(sql, columnIndexes);
	}

	
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		
		return pst.executeUpdate(sql, columnNames);
	}

	
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		
		return pst.execute(sql, autoGeneratedKeys);
	}

	
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		
		return pst.execute(sql, columnIndexes);
	}

	
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		
		return pst.execute(sql, columnNames);
	}

	
	public int getResultSetHoldability() throws SQLException {
		
		return pst.getResultSetHoldability();
	}

	
	public boolean isClosed() throws SQLException {
		
		return pst.isClosed();
	}

	
	public void setPoolable(boolean poolable) throws SQLException {
		
		pst.setPoolable(poolable);
	}

	
	public boolean isPoolable() throws SQLException {
		
		return pst.isPoolable();
	}

	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		
		return pst.unwrap(iface);
	}

	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		
		return pst.isWrapperFor(iface);
	}

	
	public ResultSet executeQuery() throws SQLException {
		DBPreparedStatementLog.funcLog("executeQuery()");
		return pst.executeQuery();
	}
	
	public int executeUpdate() throws SQLException {
		DBPreparedStatementLog.funcLog("executeUpdate()");
		return pst.executeUpdate();
	}

	public boolean execute() throws SQLException {
		DBPreparedStatementLog.funcLog("execute()");
		return pst.execute();
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		
		pst.setNull(parameterIndex, sqlType);
	}

	
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		
		pst.setBoolean(parameterIndex, x);
	}

	
	public void setByte(int parameterIndex, byte x) throws SQLException {
		
		pst.setByte(parameterIndex, x);
	}

	
	public void setShort(int parameterIndex, short x) throws SQLException {
		
		pst.setShort(parameterIndex, x);
	}

	
	public void setInt(int parameterIndex, int x) throws SQLException {
		
		pst.setInt(parameterIndex, x);
	}

	
	public void setLong(int parameterIndex, long x) throws SQLException {
		
		pst.setLong(parameterIndex, x);
	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
		
		pst.setFloat(parameterIndex, x);
	}

	
	public void setDouble(int parameterIndex, double x) throws SQLException {
		
		pst.setDouble(parameterIndex, x);
	}

	
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		
		pst.setBigDecimal(parameterIndex, x);
	}

	
	public void setString(int parameterIndex, String x) throws SQLException {
		
		pst.setString(parameterIndex, x);
	}

	
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		
		pst.setBytes(parameterIndex, x);
	}

	
	public void setDate(int parameterIndex, Date x) throws SQLException {
		
		pst.setDate(parameterIndex, x);
	}

	
	public void setTime(int parameterIndex, Time x) throws SQLException {
		
		pst.setTime(parameterIndex, x);
	}

	
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		
		pst.setTimestamp(parameterIndex, x);
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		
		pst.setAsciiStream(parameterIndex, x, length);
	}

	@SuppressWarnings("deprecation")
	
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		
		pst.setUnicodeStream(parameterIndex, x, length);
	}

	
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		
		pst.setBinaryStream(parameterIndex, x, length);
	}

	
	public void clearParameters() throws SQLException {
		
		pst.clearParameters();
	}

	
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		
		pst.setObject(parameterIndex, x, targetSqlType);
	}

	
	public void setObject(int parameterIndex, Object x) throws SQLException {
		
		pst.setObject(parameterIndex, x);
	}

	
	
	public void addBatch() throws SQLException {
		
		pst.addBatch();
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		
		pst.setCharacterStream(parameterIndex, reader, length);
	}

	
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		
		pst.setRef(parameterIndex, x);
	}

	
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		
		pst.setBlob(parameterIndex, x);
	}

	
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		
		pst.setClob(parameterIndex, x);
	}

	
	public void setArray(int parameterIndex, Array x) throws SQLException {
		
		pst.setArray(parameterIndex, x);
	}

	
	public ResultSetMetaData getMetaData() throws SQLException {
		
		return pst.getMetaData();
	}

	
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		
		pst.setDate(parameterIndex, x, cal);
	}

	
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		
		pst.setTime(parameterIndex, x, cal);
	}

	
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		
		pst.setTimestamp(parameterIndex, x, cal);
	}

	
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		
		pst.setNull(parameterIndex, sqlType, typeName);
	}

	
	public void setURL(int parameterIndex, URL x) throws SQLException {
		
		pst.setURL(parameterIndex, x);
	}

	
	public ParameterMetaData getParameterMetaData() throws SQLException {
		
		return pst.getParameterMetaData();
	}

	
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		
		pst.setRowId(parameterIndex, x);
	}

	
	public void setNString(int parameterIndex, String value) throws SQLException {
		
		pst.setNString(parameterIndex, value);
	}

	
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		
		pst.setNCharacterStream(parameterIndex, value, length);
	}

	
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		
		pst.setNClob(parameterIndex, value);
	}

	
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		
		pst.setClob(parameterIndex, reader, length);
	}

	
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		
		pst.setBlob(parameterIndex, inputStream, length);
	}

	
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		
		pst.setNClob(parameterIndex, reader, length);
	}

	
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		
		pst.setSQLXML(parameterIndex, xmlObject);
	}

	
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		
		pst.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		
		pst.setAsciiStream(parameterIndex, x, length);
	}

	
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		
		pst.setBinaryStream(parameterIndex, x, length);
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		
		pst.setCharacterStream(parameterIndex, reader, length);
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		
		pst.setAsciiStream(parameterIndex, x);
	}

	
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		
		pst.setBinaryStream(parameterIndex, x);
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		
		pst.setCharacterStream(parameterIndex, reader);
	}

	
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		
		pst.setNCharacterStream(parameterIndex, value);
	}

	
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		
		pst.setClob(parameterIndex, reader);
	}

	
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		
		pst.setBlob(parameterIndex, inputStream);
	}

	
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		
		pst.setNClob(parameterIndex, reader);
	}


	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}