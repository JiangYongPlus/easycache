/*
 * jiang yong 2015.04
 * DBQueryPreparedStatement.java
 * if the sql is not supported, create a DBQueryPreparedStatement for query result cache
 */
package com.easycache.sqlclient.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.easycache.utility.IMDGString;
import com.easycache.sqlclient.log.DBQueryPSTLog;
import com.easycache.sqlclient.type.DataType;
import com.easycache.sqlclient.type.SqlKind;

public class DBQueryPreparedStatement implements java.sql.PreparedStatement{
	
	protected PreparedStatement pst = null;
	protected IMDGSqlFilter imdgSqlFilter = new IMDGSqlFilter();
	protected boolean closed = false;
	protected String sqlsentence = null;
	protected SqlKind sqlkind = null;
	protected Connection connection = null;
	protected int argucnt = 0;
	protected ArrayList<String> arguNameList = null;
	protected ArrayList<String> arguValueList = new ArrayList<String>();
	private static ConcurrentHashMap<String, ArrayList<String>> arguMap = new ConcurrentHashMap<String, ArrayList<String>>(100);
	protected String[] originalSql = null;
	protected Statement dbStatement = null;
	private static HazelcastInstance hazelcast = null;
	
	public DBQueryPreparedStatement(String sql, Connection c) throws SQLException{
		//jiang yong 2014-11-2 for query result cache
		if((hazelcast = Hazelcast.getHazelcastInstanceByName("IMDG")) == null) {
			Config config = new Config();
			config.setInstanceName("IMDG");
			hazelcast = Hazelcast.newHazelcastInstance(config);
		}
		//added done
		
		sqlsentence = sql;
		connection = c;
		dbStatement = c.createStatement();
		
		if (sql.endsWith("?")) {
			sql = sql + " ";
		}
		originalSql = sql.split("\\?");
		if (originalSql.length > 1) {
			arguValueList.clear();
			argucnt = originalSql.length - 1;
			for (int i = 0; i < argucnt; i++) {
				arguValueList.add(null);
			}
		}
	}
	
	private void CheckParamIndex(int paramIndex) throws SQLException {
		if(paramIndex < 1 || paramIndex > argucnt) {
			throw new SQLException("ilegal parameter index");
		}
	}
	
	private String SQLConcat() {
		if (originalSql == null) return sqlsentence;
		StringBuffer buffer = new StringBuffer(originalSql[0]);
		if(arguValueList.size() > 0) {
			int len = arguValueList.size();
			for(int i = 0; i < len; ++i) {
				buffer.append(arguValueList.get(i));
				buffer.append(originalSql[i+1]);
			}
		}
		return buffer.toString().trim();
	}
	
	public ResultSet executeQuery(String sql) throws SQLException {
		
		DBQueryPSTLog.funcNotImplemented("executeQuery(String sql)");
		return null;
	}

	
	public int executeUpdate(String sql) throws SQLException {
		
		DBQueryPSTLog.funcNotImplemented("executeUpdate()");
		return 0;
	}

	
	public void close() throws SQLException {
		
		closed = true;
		originalSql = null;
		sqlsentence = null;
		arguNameList = null;
		arguValueList = null;
		connection = null;
		dbStatement.close();
	}

	
	public int getMaxFieldSize() throws SQLException {
		
		return 0;
	}

	
	public void setMaxFieldSize(int max) throws SQLException {
		
	}

	
	public int getMaxRows() throws SQLException {
		
		return 0;
	}

	
	public void setMaxRows(int max) throws SQLException {
		
	}

	
	public void setEscapeProcessing(boolean enable) throws SQLException {
		
	}

	
	public int getQueryTimeout() throws SQLException {
		
		return 0;
	}

	
	public void setQueryTimeout(int seconds) throws SQLException {
		
	}

	
	public void cancel() throws SQLException {
		
	}

	
	public SQLWarning getWarnings() throws SQLException {
		
		return null;
	}

	
	public void clearWarnings() throws SQLException {
		
	}

	
	public void setCursorName(String name) throws SQLException {
		
	}

	
	public boolean execute(String sql) throws SQLException {
		
		return false;
	}

	
	public ResultSet getResultSet() throws SQLException {
		
		return null;
	}

	
	public int getUpdateCount() throws SQLException {
		
		return 0;
	}

	
	public boolean getMoreResults() throws SQLException {
		
		return false;
	}

	
	public void setFetchDirection(int direction) throws SQLException {
		
	}

	
	public int getFetchDirection() throws SQLException {
		
		return 0;
	}

	
	public void setFetchSize(int rows) throws SQLException {
		
	}

	
	public int getFetchSize() throws SQLException {
		
		return 0;
	}

	
	public int getResultSetConcurrency() throws SQLException {
		
		return 0;
	}

	
	public int getResultSetType() throws SQLException {
		
		return 0;
	}

	
	public void addBatch(String sql) throws SQLException {
		
	}

	
	public void clearBatch() throws SQLException {
		
	}

	
	public int[] executeBatch() throws SQLException {
		
		return null;
	}

	
	public Connection getConnection() throws SQLException {
		
		return this.connection;
	}

	
	public boolean getMoreResults(int current) throws SQLException {
		
		return false;
	}

	
	public ResultSet getGeneratedKeys() throws SQLException {
		
		return null;
	}

	
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		
		return 0;
	}

	
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		
		return 0;
	}

	
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		
		return 0;
	}

	
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		
		return false;
	}

	
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		
		return false;
	}

	
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		
		return false;
	}

	
	public int getResultSetHoldability() throws SQLException {
		
		return 0;
	}

	
	public boolean isClosed() throws SQLException {
		
		return closed;
	}

	
	public void setPoolable(boolean poolable) throws SQLException {
		
	}

	
	public boolean isPoolable() throws SQLException {
		
		return false;
	}

	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		
		return null;
	}

	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		
		return false;
	}

	protected void checkClosed() throws SQLException {
		if (this.closed) {
			throw new SQLException("SQL_STATE_CONNECTION_NOT_OPEN");
		}
	}
	
	//jiang yong 2014-11-2 for query result cache
	public IMDGResult resultSetToResult(IMDGResultSet imdgResultSet){
		IMDGResult imdgResult = new IMDGResult();
//		imdgResult.setImdgResultInfo(true);
		imdgResult.resultCols = imdgResultSet.resultCols;
		imdgResult.resultRows = imdgResultSet.resultRows;
		imdgResult.resultCurrent = imdgResultSet.resultCurrent;
		imdgResult.lastRead = imdgResultSet.lastRead;
		for(Map.Entry<String, Integer> entry: imdgResultSet.colsNameMap.entrySet()){
			imdgResult.colsNameMap.put(entry.getKey(), entry.getValue());
		}
		for(DataType.Type colsType : imdgResultSet.colsTypeList){
			imdgResult.colsTypeList.add(colsType);
		}
		for(String colsTab : imdgResultSet.colsTabList){
			imdgResult.colsTabList.add(colsTab);
		}
		for(String colsDB: imdgResultSet.colsDBList){
			imdgResult.colsDBList.add(colsDB);
		}
		for(int i = 0; i < imdgResultSet.colsValuesList.size(); i++){
			imdgResult.colsValuesList.add(new ArrayList<Object>());
			ArrayList<Object> rowlist = imdgResultSet.colsValuesList.get(i);
			for(int j = 0; j < rowlist.size(); j++){
				imdgResult.colsValuesList.get(i).add(rowlist.get(j));
			}
		}
		return imdgResult;
	}
	
	public IMDGResultSet resultToResultSet(IMDGResult imdgResult){
		try{
			IMDGResultSet imdgResultSet = new IMDGResultSet(this);
			imdgResultSet.resultCols = imdgResult.resultCols;
			imdgResultSet.resultRows = imdgResult.resultRows;
			imdgResultSet.resultCurrent = imdgResult.resultCurrent;
			imdgResultSet.lastRead = imdgResult.lastRead;
			for(Map.Entry<String, Integer> entry: imdgResult.colsNameMap.entrySet()){
				imdgResultSet.colsNameMap.put(entry.getKey(), entry.getValue());
			}
			for(DataType.Type colsType : imdgResult.colsTypeList){
				imdgResultSet.colsTypeList.add(colsType);
			}
			for(String colsTab : imdgResult.colsTabList){
				imdgResultSet.colsTabList.add(colsTab);
			}
			for(String colsDB: imdgResult.colsDBList){
				imdgResultSet.colsDBList.add(colsDB);
			}
			for(int i = 0; i < imdgResult.colsValuesList.size(); i++){
				imdgResultSet.colsValuesList.add(new ArrayList<Object>());
				ArrayList<Object> rowlist = imdgResult.colsValuesList.get(i);
				for(int j = 0; j < rowlist.size(); j++){
					imdgResultSet.colsValuesList.get(i).add(rowlist.get(j));
				}
			}
			return imdgResultSet;
		}catch(SQLException e){
			e.printStackTrace();
			return null;
		}
	}
	
	public IMDGResult dbResultSetToIMDGResult(ResultSet dbResultSet){
		try{
			IMDGResult imdgResult = new IMDGResult();
//			imdgResult.setImdgResultInfo(true);
			ResultSetMetaData rsData = dbResultSet.getMetaData();
			ArrayList<String> namesList = new ArrayList<String>();
			ArrayList<String> typeNameList = new ArrayList<String>();
			ArrayList<String> colsTabList = new ArrayList<String>();
			ArrayList<String> colsDBList = new ArrayList<String>();
			ArrayList<List<Object> > colsValueList = new ArrayList<List<Object> >();

			int columncount = rsData.getColumnCount();
			for (int i = 1; i <= columncount; i++) {
				String columnName = rsData.getColumnName(i).toUpperCase();
				namesList.add(columnName);

				String className = rsData.getColumnClassName(i).toUpperCase();
				typeNameList.add(className);

				colsTabList.add(rsData.getTableName(i).toUpperCase());
				colsDBList.add(rsData.getCatalogName(i).toUpperCase());
			}
			imdgResult.addMetaData(namesList, typeNameList, colsTabList, colsDBList);
			while (dbResultSet.next()) {
				ArrayList<Object>  rowlist = new ArrayList<Object>();
				columncount = rsData.getColumnCount();
				for (int i = 1; i <= columncount; i++) {
					rowlist.add(dbResultSet.getObject(i));
				}
				colsValueList.add((List<Object>)rowlist);
			}
			imdgResult.addRowValueData(colsValueList);
			imdgResult.checkResult();
			dbResultSet.close();
			return imdgResult;
		}catch(SQLException e){
			e.printStackTrace();
			return null;
		}	
	}
	
	public ResultSet executeQuery() throws SQLException {
		if(imdgSqlFilter.getCacheSwitch() == false){
			return executeQueryNoQueryCache();
		} else {
			if(imdgSqlFilter.supportedQueryResultCache(sqlsentence)){
				checkClosed();
				IMap<String, IMDGResult> map = hazelcast.getMap(IMDGString.QUERY_RESULT_CACHE);
				String sql = SQLConcat();
//				System.out.println("before cached: " +  sql);
				if (map.containsKey(sql)) {
//					System.out.println(sql);
					IMDGResult rst = map.get(sql);
					return rst != null? resultToResultSet(rst) : null;
				} else {
					ResultSet rst = dbStatement.executeQuery(sql);
					if (rst != null) {
						map.put(sql, dbResultSetToIMDGResult(rst));
						return resultToResultSet(map.get(sql));
					} else {
						map.put(sql, null);
						return null;
					}
					
				}// map.containsKey(sql) end
			} else {
				return executeQueryNoQueryCache();
			}//supportedQueryResultCache(sqlsentence) end
		}//getCacheSwitch().equals("false") end
	}
	
	public ResultSet executeQueryNoQueryCache() throws SQLException {
		checkClosed();
		String sql = SQLConcat();
		return dbStatement.executeQuery(sql);
	}
			
	public int executeUpdate() throws SQLException {
		checkClosed();
		String sql = SQLConcat();
		return dbStatement.executeUpdate(sql);
	}

	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLException("No Implemented.");
		
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		// most 1024 bytes
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		byte[] bytes = new byte[1025];
		try {
			x.read(bytes, 0, 1024);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(bytes));
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		// different with MySQL
		// read from stream into a string
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		byte[] bytes = new byte[length + 1];
		try {
			x.read(bytes, 0, length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(bytes));
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		// same problem with the above
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		byte[] bytes = new byte[(int) (length+1L)];
		try {
			x.read(bytes, 0, (int) length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(bytes));
	}

	
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setValue(parameterIndex, null);
		} else {
			setValue(parameterIndex, String.valueOf(x));
		}
	}

	
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		setAsciiStream(parameterIndex, x);
	}

	
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		// same problem with the above
		setAsciiStream(parameterIndex, x, length);
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		setAsciiStream(parameterIndex, x, length);
	}

	
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
		} else {
			setString(parameterIndex, new String(x.getBytes(0, (int)x.length())));
		}
	}

	
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		setAsciiStream(parameterIndex, inputStream);
	}

	
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		setAsciiStream(parameterIndex, inputStream, length);
	}

	
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if(x) {
			setValue(parameterIndex, "true");
		} else {
			setValue(parameterIndex, "false");
		}
	}

	
	public void setByte(int parameterIndex, byte x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
		} else {
			setString(parameterIndex, new String(x));
		}
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		// most 1024 bytes
		CheckParamIndex(parameterIndex);
		if (reader == null) {
			setValue(parameterIndex, null);
			return;
		}
		char[] buffer = new char[1025];
		try {
			reader.read(buffer, 0, 1024);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(buffer));
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		// same problem with the above
		CheckParamIndex(parameterIndex);
		if (reader == null) {
			setString(parameterIndex, null);
			return;
		}
		char[] buffer = new char[length+1];
		try {
			reader.read(buffer, 0, length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(buffer));
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		setCharacterStream(parameterIndex, reader, (int)length);
	}

	public void setClob(int parameterIndex, Clob x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
		} else {
			setString(parameterIndex, x.getSubString(1L, (int)x.length()));
		}
	}

	
	
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader);
	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		setCharacterStream(parameterIndex, reader, (int)length);
	}

	public void setDate(int parameterIndex, Date x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		} 
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		setString(parameterIndex, dateFormatter.format(x));
	}

	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		setDate(parameterIndex, x);
	}

	public void setDouble(int parameterIndex, double x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
//		parameterStrings[parameterIndex-1] = String.valueOf(x);
	}

	public void setInt(int parameterIndex, int x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	public void setLong(int parameterIndex, long x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}


	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		setCharacterStream(parameterIndex, value);
	}

	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		setCharacterStream(parameterIndex, value, (int)length);		
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		// no supported yet
		throw new SQLException("No Implemented.");
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader);
	}

	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setNString(int parameterIndex, String value)
			throws SQLException {
		setString(parameterIndex, value);
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, null);
	}

	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		setNull(parameterIndex, sqlType);
	}

	//TODO: datatype 集中管理
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.OTHER);
		} else {
			if (x instanceof Byte) {
				setInt(parameterIndex, ((Byte) x).intValue());
			} else if (x instanceof String) {
				setString(parameterIndex, (String) x);
			} else if (x instanceof BigDecimal) {
				setBigDecimal(parameterIndex, (BigDecimal) x);
			} else if (x instanceof Short) {
				setShort(parameterIndex, ((Short) x).shortValue());
			} else if (x instanceof Integer) {
				setInt(parameterIndex, ((Integer) x).intValue());
			} else if (x instanceof Long) {
				setLong(parameterIndex, ((Long) x).longValue());
			} else if (x instanceof Float) {
				setFloat(parameterIndex, ((Float) x).floatValue());
			} else if (x instanceof Double) {
				setDouble(parameterIndex, ((Double) x).doubleValue());
			} else if (x instanceof byte[]) {
				setBytes(parameterIndex, (byte[]) x);
			} else if (x instanceof java.sql.Date) {
				setDate(parameterIndex, (java.sql.Date) x);
			} else if (x instanceof Time) {
				setTime(parameterIndex, (Time) x);
			} else if (x instanceof Timestamp) {
				setTimestamp(parameterIndex, (Timestamp) x);
			} else if (x instanceof Boolean) {
				setBoolean(parameterIndex, ((Boolean) x)
						.booleanValue());
			} else if (x instanceof InputStream) {
				setBinaryStream(parameterIndex, (InputStream) x, -1);
			} else if (x instanceof java.sql.Blob) {
				setBlob(parameterIndex, (java.sql.Blob) x);
			} else if (x instanceof java.sql.Clob) {
				setClob(parameterIndex, (java.sql.Clob) x);
			} else if (x instanceof java.util.Date) {
				setTimestamp(parameterIndex, new Timestamp(((java.util.Date) x).getTime()));
			} else if (x instanceof BigInteger) {
				setString(parameterIndex, x.toString());
			} else {
				setSerializableObject(parameterIndex, x);
			}
		}
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		setObject(parameterIndex, x);
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub
		
	}




	public void setRef(int parameterIndex, Ref x) throws SQLException {
			throw new SQLException("No Implemented.");
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		// no supported yet
		throw new SQLException("No Implemented.");
	}

	private final void setSerializableObject(int parameterIndex,
			Object parameterObj) throws SQLException {
		try {
			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
			objectOut.writeObject(parameterObj);
			objectOut.flush();
			objectOut.close();
			bytesOut.flush();
			bytesOut.close();

			byte[] buf = bytesOut.toByteArray();
			ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);
			setBinaryStream(parameterIndex, bytesIn, buf.length);
		} catch (Exception ex) {
			throw new SQLException(ex.getMessage());
		}
	}

	public void setShort(int parameterIndex, short x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setString(int parameterIndex, String x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setValue(parameterIndex, null);
			return;
		} else {
			arguValueList.set(parameterIndex - 1, "'" + x + "'");
		}
	}

	public void setTime(int parameterIndex, Time x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		setString(parameterIndex, x.toString());
	}

	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		setTime(parameterIndex, x);
	}

	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String time1 = formatter.format(x);
		setString(parameterIndex, time1);
	}

	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		setTimestamp(parameterIndex, x);
	}

	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		setAsciiStream(parameterIndex, x, length);
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
		if (x != null) {
			setString(parameterIndex, x.toString());
		} else {
			setNull(parameterIndex, Types.CHAR);
		}
	}
	
	private void setValue(int parameterIndex, String value) {
			if(value == null) {
				arguValueList.set(parameterIndex - 1, "null");
			} else {
				arguValueList.set(parameterIndex - 1, value);
			}
	}


	@Override
	public void clearParameters() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean execute() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void addBatch() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
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
	