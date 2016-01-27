package com.easycache.sqlclient.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import com.easycache.sqlclient.type.DataType;

public class IMDGMetaData implements java.sql.ResultSetMetaData {
	private int resultCols = 0;
	private HashMap<String, Integer> colsNameMap = new HashMap<String, Integer>();
	private ArrayList<DataType.Type> colsTypeList = new ArrayList<DataType.Type>();
	private ArrayList<String> colsTabList = new ArrayList<String>();
	private ArrayList<String> colsDBList = new ArrayList<String>();
	
	private void ErrorDump() {
		System.out.println("==================IMDGMetaData ErrorDump====================");
		System.out.println("resultCols:" + resultCols);

		LinkedList<Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(colsNameMap.entrySet());
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				Comparable<Integer> v1 = o1.getValue();
				Integer v2 = o2.getValue();
				if (v1 == null) {
					if (v2 == null) {
						return 0;
					} else {
						return -1;
					}
				} else {
					if (v2 == null) {
						return 1;
					} else {
						return v1.compareTo(v2);
					}
				}
			}
		});
		Iterator<Entry<String, Integer>> iterator = list.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			Entry<String, Integer> entry = iterator.next();
			System.out.println(
					entry.getKey() + " : " + entry.getValue() + " : " + colsTypeList.get(i) + " : " + colsTabList.get(i++));
		}
	}
	
	private void CheckColumnIndex(int columnIndex) throws SQLException {
		if (columnIndex > resultCols || columnIndex < 1) {
			System.out.println("columnIndex : " + columnIndex);
			ErrorDump();
			throw new SQLException("Out of Range of ColumnIndex");
		}
	}

	public IMDGMetaData(HashMap<String, Integer> NameMap, ArrayList<DataType.Type> TypeList, ArrayList<String> TabList, ArrayList<String> DBList) {
		this.resultCols = NameMap.size();
		if(NameMap != null) {
			for(java.util.Map.Entry<String, Integer> entry : NameMap.entrySet()) {
				this.colsNameMap.put(entry.getKey(), entry.getValue());
			}
		}
		if(TabList != null) {
			for (DataType.Type type : TypeList) {
				this.colsTypeList.add(type);
			}
		}
		if(TabList != null) {
			for (String string : TabList) {
				this.colsTabList.add(string);
			}
		}
		if(DBList != null) {
			for (String string : DBList) {
				this.colsDBList.add(string);
			}
		}
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("No Implemented");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("No Implemented");
	}

	public int getColumnCount() throws SQLException {
		return resultCols;
	}

	public boolean isAutoIncrement(int column) throws SQLException {
		CheckColumnIndex(column);
		throw new SQLException("No Implemented");
	}

	public boolean isCaseSensitive(int column) throws SQLException {
		CheckColumnIndex(column);
		DataType.Type type = colsTypeList.get(column - 1);
		switch (type) {
		case STRING:
			return true;
		default:
			return false;
		}
	}

	public boolean isSearchable(int column) throws SQLException {
		return true;
	}

	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	public int isNullable(int column) throws SQLException {
		CheckColumnIndex(column);
		return ResultSetMetaData.columnNullableUnknown;
	}

	public boolean isSigned(int column) throws SQLException {
		CheckColumnIndex(column);
		DataType.Type type = colsTypeList.get(column - 1);
		switch (type) {
		case INTEGER:
		case SHORT:
		case LONG:
		case FLOAT:
		case DOUBLE:
			return true;
		default:
			return false;
		}
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLException("No Implemented");
	}

	public String getColumnLabel(int column) throws SQLException {
		// TODO return the designated name
		CheckColumnIndex(column);
		for (java.util.Map.Entry<String, Integer> entry : colsNameMap.entrySet()) {
			if (entry.getValue().intValue() == column) {
				return entry.getKey();
			}
		}
		throw new SQLException("No this column");
	}

	public String getColumnName(int column) throws SQLException {
		// TODO return the designated name
		CheckColumnIndex(column);
		return getColumnLabel(column);
	}

	public String getSchemaName(int column) throws SQLException {
		return "";
	}

	public int getPrecision(int column) throws SQLException {
		// no supported
		CheckColumnIndex(column);
		throw new SQLException("No Implemented");
	}

	public int getScale(int column) throws SQLException {
		// no supported
		CheckColumnIndex(column);
		throw new SQLException("No Implemented");
	}

	public String getTableName(int column) throws SQLException {
		CheckColumnIndex(column);
		return colsTabList.get(column - 1);
	}

	public String getCatalogName(int column) throws SQLException {
		CheckColumnIndex(column);
		return colsDBList.get(column - 1);
	}

	public int getColumnType(int column) throws SQLException {
		CheckColumnIndex(column);
		DataType.Type type = colsTypeList.get(column - 1);
		switch (type) {
		case INTEGER:
		case SHORT:
		case LONG:
			return java.sql.Types.INTEGER;
		case FLOAT:
			return java.sql.Types.FLOAT;
		case DOUBLE:
			return java.sql.Types.DOUBLE;
		case STRING:
			return java.sql.Types.VARCHAR;
		case BOOLEAN:
			return java.sql.Types.BOOLEAN;
		case DATE:
			return java.sql.Types.DATE;
		case TIME:
			return java.sql.Types.TIME;
		case TIMESTAMP:
			return java.sql.Types.TIMESTAMP;
		case BIGDECIMAL:
			return java.sql.Types.DECIMAL;
		default:
			throw new SQLException("Unknow Typel:" + column);
		}
	}

	public String getColumnTypeName(int column) throws SQLException {
		CheckColumnIndex(column);
		// the type name does not match with the real type in DB perfectly
		DataType.Type type = colsTypeList.get(column - 1);
		switch (type) {
		case INTEGER:
		case SHORT:
		case LONG:
			return "INTEGER";
		case FLOAT:
			return "FLOAT";
		case DOUBLE:
			return "DOUBLE";
		case STRING:
			return "VARCHAR";
		case BOOLEAN:
			return "BOOLEAN";
		case DATE:
			return "DATE";
		case TIME:
			return "TIME";
		case TIMESTAMP:
			return "TIMESTAMP";
		case BIGDECIMAL:
			return "DECIMAL";
		default:
			throw new SQLException("Unknow Type:" + column + " " + type);
		}
	}

	public boolean isReadOnly(int column) throws SQLException {
		CheckColumnIndex(column);
		throw new SQLException("No Implemented");
	}

	public boolean isWritable(int column) throws SQLException {
		CheckColumnIndex(column);
		throw new SQLException("No Implemented");
	}

	public boolean isDefinitelyWritable(int column) throws SQLException {
		CheckColumnIndex(column);
		throw new SQLException("No Implemented");
	}

	public String getColumnClassName(int column) throws SQLException {
		CheckColumnIndex(column);
		throw new SQLException("No Implemented");
	}
}
