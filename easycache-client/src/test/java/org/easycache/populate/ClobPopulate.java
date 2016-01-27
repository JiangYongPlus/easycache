package org.easycache.populate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;

import org.easycache.jdbc.DBHelper;

public class ClobPopulate {
	
	private static Connection con = null;
	private static Random rand = new Random();
	private static final int clobNum = 20;
	
	public static void createTables(){
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE TABLE CLOB_XX1 ( ID int PRIMARY KEY, NAME varchar(40), CLOBATTR CLOB)");
			statement.executeUpdate();
			System.out.println("Created table CLOB_XX1");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create table: CLOB_XX1");
			ex.printStackTrace();
			System.exit(1);
		}
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE SEQUENCE CLOB_XX1_SEQ minvalue 1 no maxvalue START WITH 1 increment by 1 no cycle no cache");
			statement.executeUpdate();
			System.out.println("Created table CLOB_XX1's SEQUENCE");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create sequence: CLOB_XX1");
			ex.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void deleteTables() {
		int i;
		String[] tables = { "CLOB_XX1" };
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
	
	public static void populateCLOBTable(){
		String name;
		try {
			PreparedStatement statement = con
					.prepareStatement("INSERT INTO CLOB_XX1(ID, NAME, CLOBATTR) VALUES (CLOB_XX1_SEQ.nextval, ?, empty_clob())");
			for (int i = 1; i <= clobNum; i++) {
				name = getRandomAString(15, 40);
				statement.setString(1, name);
				statement.executeUpdate();
			}
			System.out.println("table CLOB_XX1 populated.");
		} catch (java.lang.Exception ex) {
			System.err.println("Unable to populate CLOB_XX1 table");
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
		populateCLOBTable();
		DBHelper.closeConnection();
		System.out.println("populated done!");
	}
	
	public static void main(String [] args) {
		start();
	}
}
