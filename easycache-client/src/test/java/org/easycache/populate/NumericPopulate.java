package org.easycache.populate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;

import org.easycache.jdbc.DBHelper;

public class NumericPopulate {
	
	private static Connection con = null;
	private static Random rand = new Random();
	private static final int NumericNum = 20;
	
	public static void createTables(){
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE TABLE NUMERIC_XX1 ( ID Numeric(20, 0) PRIMARY KEY, NAME varchar(40), VALUE Numeric(20,2))");
			statement.executeUpdate();
			System.out.println("Created table NUMERIC_XX1");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create table: NUMERIC_XX1");
			ex.printStackTrace();
			System.exit(1);
		}
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE SEQUENCE NUMERIC_XX1_SEQ minvalue 1 no maxvalue START WITH 1 increment by 1 no cycle no cache");
			statement.executeUpdate();
			System.out.println("Created table NUMERIC_XX1's SEQUENCE");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create sequence: NUMERIC_XX1");
			ex.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void deleteTables() {
		int i;
		String[] tables = { "NUMERIC_XX1" };
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
				PreparedStatement statement = con.prepareStatement("DROP SEQUENCE " + tables[i]+"_SEQ");
				statement.executeUpdate();
				System.out.println("Dropped table sequence " + tables[i]);
			} catch (java.lang.Exception ex) {
				System.out.println("Unable to drop table sequence " + tables[i]);
			}
		}
		System.out.println("Done deleting tables!");
	}
	
	public static void populateNUMERICTable(){
		String name;
		BigDecimal value = new BigDecimal(2.1);
		try {
			PreparedStatement statement = con
					.prepareStatement("INSERT INTO NUMERIC_XX1(ID, NAME, VALUE) VALUES (NUMERIC_XX1_SEQ.nextval, ?, ?)");
			for (int i = 1; i <= NumericNum; i++) {
				name = getRandomAString(15, 40);
				statement.setString(1, name);
				statement.setBigDecimal(2, value);
				statement.executeUpdate();
			}
			System.out.println("table NUMERIC_XX1 populated.");
		} catch (java.lang.Exception ex) {
			System.err.println("Unable to populate NUMERIC_XX1 table");
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
		final char[] chars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
				'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C',
				'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
				'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#', '$', '%', '^', '&', '*', '(',
				')', '_', '-', '=', '+', '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/',
				'~', ' ' }; 
		int strlen = rand.nextInt(max-min) + min;
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
		populateNUMERICTable();
		DBHelper.closeConnection();
		System.out.println("populated done!");
	}
	
	public static void main(String [] args) {
		start();
	}
}
