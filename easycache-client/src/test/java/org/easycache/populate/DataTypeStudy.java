package org.easycache.populate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

import org.easycache.jdbc.DBHelper;

public class DataTypeStudy {

	private static Connection con = null;
	private static Random rand = new Random();
	private static final int dataNum = 20;

	public static void createTables(){
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE TABLE WholeDataType (id int PRIMARY KEY, dbChar Char(40), dbVarchar varchar(40), dbText Text, dbBit Bit(4)" 
							+ ",dbTinyInt tinyint, dbBigInt bigint, dbSmallInt smallint, dbInt integer, dbNumeric numeric(20,2), dbDecimal decimal(20,2), dbNumber number(20,2), dbReal real"
							+ ", dbFloat float, dbDouble double precision, dbVarBinary varbinary(10), dbDate date, dbTime time, dbTimeStamp timestamp"
							+", dbIntervalYearToMonth interval year to month, dbIntervalDayToSecond interval day to second, dbBoolean boolean, dbClob clob, dbBlob blob)");
			statement.executeUpdate();
			System.out.println("Created table WholeDataType");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create table: WholeDataType");
			ex.printStackTrace();
			System.exit(1);
		}
		
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE SEQUENCE WholeDataType_SEQ minvalue 1 no maxvalue START WITH 1 increment by 1 no cycle no cache");
			statement.executeUpdate();
			System.out.println("Created table WholeDataType's SEQUENCE");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create sequence: WholeDataType");
			ex.printStackTrace();
			System.exit(1);
		}
	}

	public static void deleteTables() {
		int i;
		String[] tables = { "WholeDataType" };
		int numTables = tables.length;

		for (i = 0; i < numTables; i++) {
			try {
				PreparedStatement statement = con.prepareStatement("DROP TABLE " + tables[i]);
				statement.executeUpdate();
				System.out.println("Dropped table " + tables[i]);
			} catch (java.lang.Exception ex) {
				System.out.println("Unable to drop table " + tables[i]);
			}

			try {
				// Delete each table sequence in the tables array
				PreparedStatement statement = con.prepareStatement("DROP SEQUENCE " + tables[i] + "_SEQ");
				statement.executeUpdate();
				System.out.println("Dropped table sequence " + tables[i]);
			} catch (java.lang.Exception ex) {
				System.out.println("Unable to drop table sequence " + tables[i]);
			}
		}
		System.out.println("Done deleting tables!");
	}

	public static void populateDataTypeTable() {
		String dbChar, dbVarChar, dbText;
		String dbBit = "B'1010'";
		short dbSmallInt = 23;
		int dbInt;
		BigDecimal dbNumeric = new BigDecimal(2.1);
		BigDecimal dbDecimal = new BigDecimal(22.11);
		BigDecimal dbNumber = new BigDecimal(33);
		Float dbReal = 100.1f;
		Float dbFloat = 1000.01f;
		Double dbDouble = 10.100;
		int dbVarBinary = 0xB1;
		boolean dbBoolean = false;
//		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
		try {
			PreparedStatement statement = con
					.prepareStatement("INSERT INTO WholeDataType(ID, dbChar, dbVarchar, dbText, dbBit"
							+ ", dbTinyInt, dbBigInt, dbSmallInt, dbInt, dbNumeric, dbDecimal, dbNumber, dbReal"
							+ ", dbFloat, dbDouble, dbVarBinary, dbDate, dbTime, dbTimeStamp"
							+ ", dbIntervalYearToMonth, dbIntervalDayToSecond, dbBoolean, dbClob, dbBlob)"
							+ " VALUES (WholeDataType_SEQ.nextval, ?, ?, ?, B'1010', 2, 3, ?, ?, ?, ?, ?, ?, ?, ?, 0xB1, ?, ?, ?, INTERVAL '1' YEAR, INTERVAL '20' DAY, ?, empty_clob(), empty_blob())");
			for (int i = 1; i <= dataNum; i++) {
				dbChar = getRandomAString(15, 40);
				dbVarChar = getRandomAString(15, 40);
				dbText = getRandomAString(15, 40);
				dbInt = getRandomInt(10, 100);
				Date dbDate = new Date(System.currentTimeMillis());
				Time dbTime = new Time(System.currentTimeMillis());
				Timestamp dbTimeStamp = new Timestamp(System.currentTimeMillis());
				
				statement.setString(1, dbChar);
				statement.setString(2, dbVarChar);
				statement.setString(3, dbText);
//				statement.setString(4, dbBit);
				statement.setShort(4, dbSmallInt);
				statement.setInt(5, dbInt);
				statement.setBigDecimal(6, dbNumeric);
				statement.setBigDecimal(7, dbDecimal);
				statement.setBigDecimal(8, dbNumber);
				statement.setFloat(9, dbReal);
				statement.setFloat(10, dbFloat);
				statement.setDouble(11, dbDouble);
//				statement.setInt(13, dbVarBinary);
				statement.setDate(12, dbDate);
				statement.setTime(13, dbTime);
				statement.setTimestamp(14, dbTimeStamp);
				statement.setObject(15, dbBoolean);
				statement.executeUpdate();
			}
			System.out.println("table WholeDataType populated.");
		} catch (java.lang.Exception ex) {
			System.err.println("Unable to populate WholeDataType table");
			ex.printStackTrace();
			System.exit(1);
		}
	}

	public static int getRandomInt(int lower, int upper) {
		return rand.nextInt(upper + 1 - lower) + lower;
	}

	public static String getRandomAString(int min, int max) {
		String newstring = new String();
		int i;
		final char[] chars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
				'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
				'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
				'Z', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '=', '+', '{', '}', '[',
				']', '|', ':', ';', ',', '.', '?', '/', '~', ' ' };
		int strlen = rand.nextInt(max - min) + min;
		for (i = 0; i < strlen; i++) {
			char c = chars[rand.nextInt(chars.length)];
			newstring = newstring.concat(String.valueOf(c));
		}
		return newstring;
	}

	public static void start() {
		con = DBHelper.getConnection();
		deleteTables();
		createTables();
		populateDataTypeTable();
		DBHelper.closeConnection();
		System.out.println("populated done!");
	}

	public static void main(String[] args) {
		start();
	}
}
