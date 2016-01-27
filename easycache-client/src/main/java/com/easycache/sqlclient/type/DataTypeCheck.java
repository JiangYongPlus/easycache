package com.easycache.sqlclient.type;

//jiang yong 2015-01-29
//add type: clob, blob
public class DataTypeCheck {
	public static DataType.Type check(String type) {
		if (type.equalsIgnoreCase("java.lang.Integer")) {
			return DataType.Type.INTEGER;
		} else if (type.equalsIgnoreCase("java.lang.Long")) {
			return DataType.Type.LONG;
		} else if (type.equalsIgnoreCase("java.lang.Float")) {
			return DataType.Type.FLOAT;
		} else if (type.equalsIgnoreCase("java.lang.Double")) {
			return DataType.Type.DOUBLE;
		} else if (type.equalsIgnoreCase("java.lang.String")) {
			return DataType.Type.STRING;
		} else if (type.equalsIgnoreCase("java.sql.Date")) {
			return DataType.Type.DATE;
		} else if (type.equalsIgnoreCase("java.sql.Time")) {
			return DataType.Type.TIME;
		} else if (type.equalsIgnoreCase("java.sql.Timestamp")) {
			return DataType.Type.TIMESTAMP;
		} else if (type.equalsIgnoreCase("java.lang.Boolean")) {
			return DataType.Type.BOOLEAN;
		} else if (type.equalsIgnoreCase("java.lang.Short")) {
			return DataType.Type.SHORT;
		} else if (type.equalsIgnoreCase("java.math.BigDecimal")) {
			return DataType.Type.BIGDECIMAL;
		} else if (type.equalsIgnoreCase("java.sql.Clob")) {
			return DataType.Type.CLOB;
		} else if (type.equalsIgnoreCase("java.sql.Blob")) {
			return DataType.Type.BLOB;
		} else if (type.equalsIgnoreCase("oracle.sql.TimeStamp")) {
			return DataType.Type.OCLTIMESTAMP;
		} else {
			return DataType.Type.UNKNOWN;
		}
	}
	public static String check(DataType.Type type) {
		switch (type) {
		case INTEGER:
			return "java.lang.Integer";
		case SHORT:
			return "java.lang.Short";
		case LONG:
			return "java.lang.Long";
		case BOOLEAN:
			return "java.lang.Boolean";
		case FLOAT:
			return "java.lang.Float";
		case DOUBLE:
			return "java.lang.Double";
		case DATE:
			return "java.sql.Date";
		case TIME:
			return "java.sql.Time";
		case TIMESTAMP:
			return "java.sql.TimeStamp";
		case STRING:
			return "java.lang.String";
		case BIGDECIMAL:
			return "java.math.BigDecimal";
		case BLOB:
			return "java.sql.Blob";
		case CLOB:
			return "java.sql.Clob";
		case OCLTIMESTAMP:
			return "oracle.sql.TimeStamp";
		default:
			return "UNKNOWN";
		}
	}
}
