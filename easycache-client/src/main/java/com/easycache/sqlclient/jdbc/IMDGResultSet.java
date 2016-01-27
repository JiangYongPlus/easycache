package com.easycache.sqlclient.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
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
import java.util.Calendar;
import java.util.Map;

import com.easycache.sqlclient.log.IMDGLog;
import com.easycache.sqlclient.type.DataType;


public class IMDGResultSet extends IMDGResult implements ResultSet {
	/*
	 * current line number
	 */
	protected Statement statement = null;
	
	//TODO 在做查询结果缓存时，把IMDGResult存入HazelCast map 中时，下面数据会丢失
	protected int fetchSize = 0;
	protected int fetchDirection = ResultSet.FETCH_FORWARD;
	protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
	
	public IMDGResultSet() {
		
	}
	public IMDGResultSet(Statement s)
			throws SQLException {
		if (s != null && !s.isClosed()) {
			statement = s;
		} else {
			ErrorDump();
			throw new SQLException("Illegal Statement");
		}
	}

	
	public boolean absolute(int row) throws SQLException {
		if (resultRows == 0) {
			return false;
		} else {
			resultCurrent = (row - 1) % resultRows;
			if (CheckAfterLast() || CheckBeforeFirst()) {
				return false;
			} else {
				return true;
			}
		}
	}
	
	
	public void afterLast() throws SQLException {
		resultCurrent = resultRows;
	}

	
	public void beforeFirst() throws SQLException {
		resultCurrent = -1;
	}

	
	public void cancelRowUpdates() throws SQLException {
		throw new SQLException("No Updatable");
	}
	
	protected boolean CheckAfterLast() {
		if (resultCurrent >= resultRows) {
			return true;
		} else {
			return false;
		}
	}

	protected boolean CheckBeforeFirst() {
		if (resultCurrent <= -1) {
			return true;
		} else {
			return false;
		}
	}

	protected void CheckColumnIndex(int columnIndex) throws SQLException {
		if (columnIndex > resultCols || columnIndex < 1) {
			throw new SQLException("Out of Range of ColumnIndex: index = " + columnIndex);
		}
	}

	
	public void clearWarnings() throws SQLException {
		// no implemented
	}

	
	public void close() throws SQLException {
		clear();
		closed = true;
	}

	
	public void deleteRow() throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public int findColumn(String columnLabel) throws SQLException {
		// TODO: need to support for the search of artifically named column  
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return colsNameMap.get(columnLabel.toUpperCase());
	}

	
	public boolean first() throws SQLException {
		resultCurrent = 0;
		if (CheckAfterLast()) {
			return false;
		} else {
			return true;
		}
	}

	
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLException("No Implemented");
	}
	
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		IMDGLog.rstFuncLog("getAsciiStream(int columnIndex)");
		byte[] b = getBytes(columnIndex);
		if (b != null) {
			return new ByteArrayInputStream(b);
		}
		return null;
	}
	
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		IMDGLog.rstFuncLog("getAsciiStream(String columnLabel)");
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getAsciiStream(colsNameMap.get(columnLabel.toUpperCase()));
	}
	

	
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		IMDGLog.rstFuncLog("getBigDecimal(int columnIndex)");
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return new BigDecimal(0);
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case BIGDECIMAL:
			return (BigDecimal)obj;
		case INTEGER:
			return new BigDecimal((Integer) obj);
		case SHORT:
			return new BigDecimal((Short) obj);
		case LONG:
			return new BigDecimal((Long) obj);
		case FLOAT:
			return new BigDecimal((Float) obj);
		case DOUBLE:
			return new BigDecimal((Double) obj);
		case STRING:
			return new BigDecimal((String) obj);
		case BOOLEAN:
		case DATE:
		case TIME:
		case TIMESTAMP:
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to BigDecimal:" +columnIndex);
		}
	}

	
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		return getBigDecimal(columnIndex);
	}

	
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getBigDecimal(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getBigDecimal(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return getAsciiStream(columnIndex);
	}

	
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getAsciiStream(columnLabel);
	}

	
	public Blob getBlob(int columnIndex) throws SQLException {
		byte[] b = getBytes(columnIndex);
		if (b == null) {
			return null;
		}
		return new com.easycache.sqlclient.jdbc.entity.Blob(b);
	}

	
	public Blob getBlob(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getBlob(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public boolean getBoolean(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return false;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case INTEGER:
			return ((Integer)obj) > 0;
		case SHORT:
			return ((Short)obj) > 0;
		case LONG:
			return ((Long)obj) > 0;
		case BOOLEAN:
			return (Boolean)obj;
		case FLOAT:
			return ((Float)obj) > 0;
		case DOUBLE:
			return ((Double)obj) > 0;
		case BIGDECIMAL:
			return ((BigDecimal)obj).intValue() > 0;
		case DATE:
		case TIME:
		case TIMESTAMP:
		case STRING:
		default:
			return false;
		}
	}

	
	public boolean getBoolean(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getBoolean(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public byte getByte(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return 0;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		if (type == DataType.Type.STRING) {
			String stringVal = (String) obj;
			if (stringVal == null || stringVal.length() == 0) {
				return 0;
			}
			stringVal = stringVal.trim();
			try {
				int decimalIndex = stringVal.indexOf(".");
				if (decimalIndex != -1) {
					double valueAsDouble = Double.parseDouble(stringVal);
					if (valueAsDouble < Byte.MIN_VALUE
							|| valueAsDouble > Byte.MAX_VALUE) {
						throw new SQLException("Out of Range of byte");
					}
					return (byte) valueAsDouble;
				}
				long valueAsLong = Long.parseLong(stringVal);

				if (valueAsLong < Byte.MIN_VALUE
						|| valueAsLong > Byte.MAX_VALUE) {
					throw new SQLException("Out of Range of byte");
				}
				return (byte) valueAsLong;
			} catch (NumberFormatException NFE) {
				throw new SQLException(
						"ResultSet.___is_out_of_range_[-127,127]_174");
			}
		} else {
			switch (type) {
			case INTEGER:
				return ((Integer) obj).byteValue();
			case SHORT:
				return ((Short) obj).byteValue();
			case LONG:
				return ((Long) obj).byteValue();
			case FLOAT:
				return ((Float) obj).byteValue();
			case DOUBLE:
				return ((Double) obj).byteValue();
			case BIGDECIMAL:
				return ((BigDecimal)obj).byteValue();
			case DATE:
			case TIME:
			case TIMESTAMP:
			default:
				ErrorDump();
				throw new SQLException("Object Type is UNKNOW: " + columnIndex);
			}
		}
	}

	
	public byte getByte(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getByte(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public byte[] getBytes(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return null;
		}
		String stringVal = obj.toString();
		return stringVal.getBytes();
	}

	

	
	public byte[] getBytes(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getBytes(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		String stringVal = getString(columnIndex);
		if (stringVal == null) {
			return null;
		} else {
			return new StringReader(stringVal);
		}
	}

	
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getCharacterStream(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public Clob getClob(int columnIndex) throws SQLException {
		String stringVal = getString(columnIndex);
		if (stringVal == null) {
			return null;
		}
		return new com.easycache.sqlclient.jdbc.entity.Clob(stringVal);
	}

	
	public Clob getClob(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getClob(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	
	public String getCursorName() throws SQLException {
		throw new SQLException("No Supported");
	}

	
	public Date getDate(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return null;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case STRING:
			return Date.valueOf((String) obj);
		case DATE:
			return (java.sql.Date)obj;
		case TIMESTAMP:
			return new java.sql.Date(((Timestamp)obj).getTime());
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to Date: index = " + columnIndex + " type = " + type);
		}
	}

	
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		// TODO: no calander used
		return getDate(columnIndex);
	}

	
	public Date getDate(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getDate(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		// TODO: no calander used
		return getDate(columnLabel);
	}

	
	public double getDouble(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return 0;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case INTEGER:
			return ((Integer) obj).doubleValue();
		case SHORT:
			return ((Short) obj).doubleValue();
		case LONG:
			return ((Long) obj).doubleValue();
		case FLOAT:
			return ((Float) obj).doubleValue();
		case DOUBLE:
			return (Double) obj;
		case STRING:
			return Double.valueOf((String) obj);
		case BIGDECIMAL:
			return ((BigDecimal)obj).doubleValue();
		case BOOLEAN:
		case DATE:
		case TIME:
		case TIMESTAMP:
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to Double: index = " + columnIndex);
		}
	}

	
	public double getDouble(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getDouble(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public int getFetchDirection() throws SQLException {
		// TODO no supported
		return this.fetchDirection;
	}

	
	public int getFetchSize() throws SQLException {
		// TODO no supported
		return fetchSize;
	}

	
	public float getFloat(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return 0;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case INTEGER:
			return ((Integer) obj).floatValue();
		case SHORT:
			return ((Short) obj).floatValue();
		case LONG:
			return ((Long) obj).floatValue();
		case FLOAT:
			return (Float) obj;
		case DOUBLE:
			return ((Double) obj).floatValue();
		case STRING:
			return Float.valueOf((String) obj);
		case BIGDECIMAL:
			return ((BigDecimal)obj).floatValue();
		case BOOLEAN:
		case DATE:
		case TIME:
		case TIMESTAMP:
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to Float: index = " + columnIndex);
		}
	}

	
	public float getFloat(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getFloat(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public int getHoldability() throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public int getInt(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return 0;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case INTEGER:
			return ((Integer) obj).intValue();
		case SHORT:
			return ((Short) obj).intValue();
		case LONG:
			return ((Long)obj).intValue();
		case FLOAT:
			return ((Float) obj).intValue();
		case DOUBLE:
			return ((Double) obj).intValue();
		case STRING:
			return Integer.valueOf((String) obj);
		case BIGDECIMAL:
			return ((BigDecimal)obj).intValue();
		case BOOLEAN:
		case DATE:
		case TIME:
		case TIMESTAMP:
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to Int: " + columnIndex);
		}
	}

	
	public int getInt(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getInt(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public long getLong(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return 0;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case INTEGER:
			return ((Integer) obj).longValue();
		case SHORT:
			return ((Short) obj).longValue();
		case LONG:
			return (Long) obj;
		case FLOAT:
			return ((Float) obj).longValue();
		case DOUBLE:
			return ((Double) obj).longValue();
		case STRING:
			return Long.valueOf((String) obj);
		case BIGDECIMAL:
			return ((BigDecimal)obj).longValue();
		case BOOLEAN:
		case DATE:
		case TIME:
		case TIMESTAMP:
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to Long: " + columnIndex);
		}
	}

	
	public long getLong(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getLong(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO no implement
		return new IMDGMetaData(colsNameMap, colsTypeList, colsTabList, colsDBList);
	}

	
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return getCharacterStream(columnIndex);
	}

	
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(columnLabel);
	}

	
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public String getNString(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	
	public String getNString(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}

	
	public Object getObject(int columnIndex) throws SQLException {
		return getValue(columnIndex);
	}

	
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		return getObject(columnIndex);
	}

	
	public Object getObject(String columnLabel) throws SQLException {
		if (colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getObject(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		return getObject(columnLabel);
	}

	
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLException("No Supported.");
	}

	
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLException("No Supported.");
	}

	
	public int getRow() throws SQLException {
		return resultCurrent + 1;
	}

	
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public short getShort(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return -1;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case INTEGER:
			return ((Integer) obj).shortValue();
		case SHORT:
			return (Short) obj;
		case LONG:
			return ((Long) obj).shortValue();
		case FLOAT:
			return ((Float) obj).shortValue();
		case DOUBLE:
			return ((Double) obj).shortValue();
		case STRING:
			return Short.valueOf((String) obj);
		case BIGDECIMAL:
			return ((BigDecimal)obj).shortValue();
		case BOOLEAN:
		case DATE:
		case TIME:
		case TIMESTAMP:
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to Short: " + columnIndex);
		}
	}

	
	public short getShort(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getShort(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException("No Implemented");
	}

	
	public Statement getStatement() throws SQLException {
		if (closed) {
			throw new SQLException(
					"Operation not allowed on closed ResultSet. Statements");
		}
		return statement;
	}

	
	public String getString(int columnIndex) throws SQLException {
		IMDGLog.rstFuncLog("getString(int columnIndex)");
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return null;
		}
		return obj.toString();
	}

	
	public String getString(String columnLabel) throws SQLException {
		IMDGLog.rstFuncLog("getString(String columnLabel)");
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getString(colsNameMap.get(columnLabel.toUpperCase()));
	}
	
	public Time getTime(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return null;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case STRING:
			return Time.valueOf((String) obj);
		case DATE:
			return new Time(((java.sql.Date) obj).getTime());
		case TIME:
			return (Time) obj;
		case TIMESTAMP:
			return new Time(((Timestamp) obj).getTime());
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to Time: " + columnIndex);
		}
	}

	
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		// TODO no timezone considered
		return getTime(columnIndex);
	}

	
	public Time getTime(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getTime(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		// TODO no timezone considered
		return getTime(columnLabel);
	}

	
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return null;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case STRING:
			return Timestamp.valueOf((String)obj);
		case DATE:
			return new Timestamp(((java.sql.Date) obj).getTime());
		case TIME:
			return new Timestamp(((Time) obj).getTime());
		case TIMESTAMP:
			return (Timestamp) obj;
		default:
			ErrorDump();
			throw new SQLException("Cann't Convert to TimeStamp: " + columnIndex);
		}
	}

	
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		// TODO no timezone considered
		return getTimestamp(columnIndex);
	}

	
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getTimestamp(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		// TODO no timezone considered
		return getTimestamp(columnLabel);
	}

	
	public int getType() throws SQLException {
		// TODO TYPE_FORWARD_ONLY
		return this.resultSetType;
	}

	
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return getAsciiStream(columnIndex);
	}

	
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getAsciiStream(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public URL getURL(int columnIndex) throws SQLException {
		Object obj = getValue(columnIndex);
		if (obj == null) {
			return null;
		}
		DataType.Type type = colsTypeList.get(columnIndex - 1);
		switch (type) {
		case STRING:
			try {
				return new URL((String)obj);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		default:
			ErrorDump();
			throw new SQLException("cann't convert to URL: " + columnIndex);
		}
	}

	
	public URL getURL(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getURL(colsNameMap.get(columnLabel.toUpperCase()));
	}

	protected Object getValue(int columnIndex) throws SQLException {
		if (CheckAfterLast() || CheckBeforeFirst()) {
			ErrorDump();
			throw new SQLException("Ilegal Operation: " + columnIndex);
		}
		CheckColumnIndex(columnIndex);
		lastRead = colsValuesList.get(resultCurrent).get(columnIndex - 1);
		return lastRead;
	}

	protected Object getValue(String columnLabel) throws SQLException {
		if(colsNameMap.get(columnLabel.toUpperCase()) == null) {
			ErrorDump();
			throw new SQLException("Wrong columnLabel: " + columnLabel);
		}
		return getValue(colsNameMap.get(columnLabel.toUpperCase()));
	}

	
	public SQLWarning getWarnings() throws SQLException {
		// no implemented
		return null;
	}

	
	public void insertRow() throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public boolean isAfterLast() throws SQLException {
		if (resultCurrent >= resultRows) {
			return true;
		} else {
			return false;
		}
	}

	
	public boolean isBeforeFirst() throws SQLException {
		if (resultCurrent == -1) {
			return true;
		} else {
			return false;
		}
	}

	
	public boolean isClosed() throws SQLException {
		return closed;
	}

	
	public boolean isFirst() throws SQLException {
		if (resultCurrent == 0) {
			return true;
		} else {
			return false;
		}
	}

	
	public boolean isLast() throws SQLException {
		if (resultCurrent == resultRows - 1) {
			return true;
		} else {
			return false;
		}
	}

	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	
	public boolean last() throws SQLException {
		resultCurrent = resultRows - 1;
		if (CheckBeforeFirst()) {
			return false;
		} else {
			return true;
		}
	}

	
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public boolean next() throws SQLException {
		++resultCurrent;
		return !CheckAfterLast();
	}

	
	public boolean previous() throws SQLException {
		if (CheckBeforeFirst()) {
			return false;
		} else {
			--resultCurrent;
			return !CheckBeforeFirst();
		}
	}

	
	public void refreshRow() throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public boolean relative(int rows) throws SQLException {
		if (resultRows == 0) {
			return false;
		} else if (resultCurrent + rows >= resultRows) {
			resultCurrent = resultRows;
			return false;
		} else if (resultCurrent + rows < 0) {
			resultCurrent = -1;
			return false;
		} else {
			resultCurrent += rows;
			return true;
		}
	}

	
	public boolean rowDeleted() throws SQLException {
		throw new SQLException("No Supported.");
	}

	
	public boolean rowInserted() throws SQLException {
		throw new SQLException("No Supported.");
	}

	
	public boolean rowUpdated() throws SQLException {
		throw new SQLException("No Supported.");
	}

	
	public void setFetchDirection(int direction) throws SQLException {
		// no supported yet
		if ((direction != FETCH_FORWARD) && (direction != FETCH_REVERSE)
				&& (direction != FETCH_UNKNOWN)) {
			throw new SQLException(
					"ResultSet.Illegal_value_for_fetch_direction_64"); //$NON-NLS-1$
		}

		this.fetchDirection = direction;

	}

	
	public void setFetchSize(int rows) throws SQLException {
		// no supported yet
		if (rows < 0) {
			throw new SQLException("ResultSet.Value_must_be_between_0_and_getMaxRows()_66");
			}
		this.fetchSize = rows;
	}

	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("No Updatable");

	}

	
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("No Updatable");

	}
		
	
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateRow() throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw new SQLException("No Updatable");
	}

	
	public boolean wasNull() throws SQLException {
		if (lastRead == null) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
}
