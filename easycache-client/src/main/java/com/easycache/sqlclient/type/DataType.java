package com.easycache.sqlclient.type;

/**
* Integer	MInt;
* Short	MTinyint;
* Short	MSmallint;
* Integer	MMediumint;
* Integer	MInteger;
* Long	MBigint;
* Boolean	MBit;
* Double	MReal;
* Double	MDouble;
* Float	MFloat;
* Long	MDecimal;
* Long	MNumeric;
* String	MChar;
* String	MVarchar;
* Date	MDate;
* Time	MTime;
* Date	MYear;
* Timestamp	MTimestamp;
* Timestamp	MDatetime;
* String	MTinyblob;
* String	MBlob;
* String	MLongblob;
* String	MTinytext;
* String	MText;
* String	MEnum;
* String	MSet;
* String	MBinary;
* String	MVarbinary;
 */

/*
 * jiang yong 2015-01-29
 * shentong datatype and jdbc datatype
 */
/**
* Integer	STInt (ShenTong int type);
* Integer STTinyint;
* Integer	STSmallint;
* Integer	STMediumint;
* Integer	STInteger;
* Long	STBigint;
* Boolean	STBit;
* Float	STReal;
* Double	STDouble;
* Double	STFloat;
* BigDecimal	STDecimal;
* BigDecimal	STNumeric;
* BigDecimal 	STNumber;
* String	STChar;
* String	STVarchar;
* String	STText;
* TimeStamp	STDate;
* Time	STTime;
* Timestamp	STTimestamp;
* Blob	STBlob;
* Clob 	STClob;
* unknow	STVarbinary;
* Blooean	STBoolean
* oracleTimeStamp Ocltimestamp (oracle dataType : timestamp)
 */

//jiang yong 2015-01-29
//add clob, blob, oclTimeSTamp
public class DataType {
	public static enum Type {
		INTEGER,SHORT,LONG,BOOLEAN,FLOAT,DOUBLE,DATE,TIME,TIMESTAMP,STRING,BIGDECIMAL,CLOB,BLOB,OCLTIMESTAMP,UNKNOWN
	};
}
