/*
 * jiang yong 2015.01-2015.02
 */
package com.easycache.sqlclient.load;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import com.easycache.sqlclient.type.DataType.Type;
import com.easycache.sqlclient.type.DataTypeCheck;
import com.hazelcast.core.IMap;

public class LoaderHelper {
	private static Set<Type> supportedDataTypeSet = new HashSet<Type>();

	static {
		supportedDataTypeSet.add(Type.INTEGER);
		supportedDataTypeSet.add(Type.SHORT);
		supportedDataTypeSet.add(Type.LONG);
		supportedDataTypeSet.add(Type.BOOLEAN);
		supportedDataTypeSet.add(Type.FLOAT);
		supportedDataTypeSet.add(Type.DOUBLE);
		supportedDataTypeSet.add(Type.DATE);
		supportedDataTypeSet.add(Type.TIME);
		supportedDataTypeSet.add(Type.TIMESTAMP);
		supportedDataTypeSet.add(Type.STRING);
		supportedDataTypeSet.add(Type.BIGDECIMAL);
		supportedDataTypeSet.add(Type.OCLTIMESTAMP);
	}
	
	public static boolean isSupportedType(Type type) {
		if (supportedDataTypeSet != null && supportedDataTypeSet.contains(type)) {
			return true;
		}
		return false;
	}
	
	@SuppressWarnings("rawtypes")
	public  static  Class getClassFromString(String classTypeName) {
		Type classType = DataTypeCheck.check(classTypeName);
		switch (classType) {
		case INTEGER:
			return Integer.class;
		case SHORT:
			return Short.class;
		case LONG:
			return Long.class;
		case BOOLEAN:
			return Boolean.class;
		case FLOAT:
			return Float.class;
		case DOUBLE:
			return Double.class;
		case DATE:
			return java.sql.Date.class;
		case TIME:
			return java.sql.Time.class;
		case TIMESTAMP:
			if (Loader.getDBType().equals("oracle")) {
				return java.sql.Date.class;
			}
			return java.sql.Timestamp.class;
		case STRING:
			return String.class;
		case BIGDECIMAL:
			if (Loader.getDBType().equals("oracle")) {
				return java.lang.Integer.class;
			}
			return java.math.BigDecimal.class;
		case OCLTIMESTAMP:
			return java.sql.Timestamp.class;
		default:
			System.out.println("warning: unsupported classTypeName" + classTypeName);
			return String.class;
		}
	}
	
	public static Object getAttributeValue(String classTypeName, ResultSet rsSet, int i) {
		Type classType = DataTypeCheck.check(classTypeName);
		try {
			switch (classType) {
			case INTEGER:
				return new Integer(rsSet.getInt(i));
			case SHORT:
				return new Short(rsSet.getShort(i));
			case LONG:
				return new Long(rsSet.getLong(i));
			case BOOLEAN:
				return new Boolean(rsSet.getBoolean(i));
			case FLOAT:
				return new Float(rsSet.getFloat(i));
			case DOUBLE:
				return new Double(rsSet.getDouble(i));
			case DATE:
				return rsSet.getDate(i);
			case TIME:
				return rsSet.getTime(i);
			case TIMESTAMP:
				if (Loader.getDBType().equals("oracle")) {
					return rsSet.getDate(i);
				}
				return rsSet.getTimestamp(i);
			case STRING:
				return rsSet.getString(i);
			case BIGDECIMAL:
				if (Loader.getDBType().equals("oracle")) {
					return new Integer(rsSet.getInt(i));
				}
				return rsSet.getBigDecimal(i);
			case OCLTIMESTAMP:
				return rsSet.getTimestamp(i);
			default:
				return rsSet.getObject(i);
			}
		} catch (Exception e) {
			System.out.println("classTypeName: " + classTypeName);
			System.err.println("SQLException :" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public static Object getAttributeValue(String classTypeName, ResultSet rsSet, String columnName) {
		Type classType = DataTypeCheck.check(classTypeName);
		try {
			switch (classType) {
			case INTEGER:
				return new Integer(rsSet.getInt(columnName));
			case SHORT:
				return new Short(rsSet.getShort(columnName));
			case LONG:
				return new Long(rsSet.getLong(columnName));
			case BOOLEAN:
				return new Boolean(rsSet.getBoolean(columnName));
			case FLOAT:
				return new Float(rsSet.getFloat(columnName));
			case DOUBLE:
				return new Double(rsSet.getDouble(columnName));
			case DATE:
				return rsSet.getDate(columnName);
			case TIME:
				return rsSet.getTime(columnName);
			case TIMESTAMP:
				if (Loader.getDBType().equals("oracle")) {
					return rsSet.getDate(columnName);
				}
				return rsSet.getTimestamp(columnName);
			case STRING:
				return rsSet.getString(columnName);
			case BIGDECIMAL:
				if (Loader.getDBType().equals("oracle")) {
					return new Integer(rsSet.getInt(columnName));
				}
				return rsSet.getBigDecimal(columnName);
			case OCLTIMESTAMP:
				return rsSet.getTimestamp(columnName);
			default:
				return rsSet.getObject(columnName);
			}
		} catch (Exception e) {
			System.out.println("classTypeName: " + classTypeName);
			System.err.println("SQLException :" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
}
