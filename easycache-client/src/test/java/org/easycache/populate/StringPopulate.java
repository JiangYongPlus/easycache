package org.easycache.populate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.Random;

import org.easycache.jdbc.DBHelper;

public class StringPopulate {
	private static Connection con = null;
	private static Random rand = new Random();
	final private static int SKNum = 3600;
	final private static int UKNum = 4800;
	final private static int batchNum = 1000;

	public static void createTables(){
		
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE TABLE STRINGKEY ( STRING_ID varchar(40) PRIMARY KEY, STRING_INFO varchar(40), STRING_VAL int)");
			statement.executeUpdate();
			System.out.println("Created table STRINGKEY");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create table: STRINGKEY");
			ex.printStackTrace();
			System.exit(1);
		}
		
		try {
			PreparedStatement statement = con
					.prepareStatement("CREATE TABLE UNIONKEY ( STRING_ID varchar(40), INT_ID int, STRING_INFO varchar(40), STRING_VAL int, PRIMARY KEY(STRING_ID, INT_ID))");
			statement.executeUpdate();
			System.out.println("Created table UNIONKEY");
		} catch (java.lang.Exception ex) {
			System.out.println("Unable to create table: UNIONKEY");
			ex.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void deleteTables() {
		int i;
		String[] tables = { "STRINGKEY", "UNIONKEY" };
		int numTables = tables.length;

		for (i = 0; i < numTables; i++) {
			try {
				// Delete each table listed in the tables array
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
	
	
	public static void populateStringKeyTable(){
		String STRING_ID, STRING_INFO;
		int STRING_VAL;
		try {
			PreparedStatement statement = con
					.prepareStatement("INSERT INTO STRINGKEY(STRING_ID,STRING_INFO,STRING_VAL) VALUES (?, ?, ?)");
			for (int i = 1; i <= SKNum; i++) {
				STRING_ID = getRandomAString(15, 40);
				STRING_INFO = getRandomAString(15, 40);
				STRING_VAL = getRandomInt(1, 92);

				statement.setString(1, STRING_ID);
				statement.setString(2, STRING_INFO);
				statement.setInt(3, STRING_VAL);
				statement.addBatch();
				if (i % batchNum == 0){
					statement.executeBatch();
					statement.clearBatch();
				}
			}
			statement.executeBatch();
			statement.clearBatch();
			System.out.println("STRINGKEY populated.");
		} catch (java.lang.Exception ex) {
			System.err.println("Unable to populate STRINGKEY table");
			ex.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void populateUnionKeyTable(){
		String STRING_ID, STRING_INFO;
		int INT_ID, STRING_VAL;
		try {
			PreparedStatement statement = con
					.prepareStatement("INSERT INTO UNIONKEY(STRING_ID, INT_ID, STRING_INFO, STRING_VAL) VALUES (?, ?, ?, ?)");
			for (int i = 1; i <= UKNum; i++) {
				STRING_ID = getRandomAString(15, 40);
				INT_ID = getRandomInt(1, 92);
				STRING_INFO = getRandomAString(15, 40);
				STRING_VAL = getRandomInt(1, 92);

				statement.setString(1, STRING_ID);
				statement.setInt(2, INT_ID);
				statement.setString(3, STRING_INFO);
				statement.setInt(4, STRING_VAL);
				statement.addBatch();
				if (i % batchNum == 0){
					statement.executeBatch();
					statement.clearBatch();
				}
			}
			statement.executeBatch();
			statement.clearBatch();
			System.out.println("UNIONKEY populated.");
		} catch (java.lang.Exception ex) {
			System.err.println("Unable to populate UNIONKEY table");
			ex.printStackTrace();
			System.exit(1);
		}
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
	
	public static String getRandomAString(int strlen) {
		String newstring = new String();
		int i;
		final char[] chars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
				'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C',
				'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
				'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#', '$', '%', '^', '&', '*', '(',
				')', '_', '-', '=', '+', '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/',
				'~', ' ' }; 
		for (i = 0; i < strlen; i++) {
			char c = chars[rand.nextInt(chars.length)];
			newstring = newstring.concat(String.valueOf(c));
		}
		return newstring;
	}
	
	public static int getRandomInt(int lower, int upper) {
		return rand.nextInt(upper + 1 - lower) + lower;
	}
	
	public static int getRandomNString(int num_digits) {
		int return_num = 0;
		for (int i = 0; i < num_digits; i++) {
			return_num += getRandomInt(0, 9) * (int) java.lang.Math.pow(10.0, (double) i);
		}
		return return_num;
	}

	public static int getRandomNString(int min, int max) {
		int strlen = rand.nextInt(max-min) + min;
		return getRandomNString(strlen);
	}

	public static String DigSyl(int D, int N) {
		int i;
		String resultString = new String();
		String Dstr = Integer.toString(D);

		if (N > Dstr.length()) {
			int padding = N - Dstr.length();
			for (i = 0; i < padding; i++)
				resultString = resultString.concat("BA");
		}
		for (i = 0; i < Dstr.length(); i++) {
			if (Dstr.charAt(i) == '0')
				resultString = resultString.concat("BA");
			else if (Dstr.charAt(i) == '1')
				resultString = resultString.concat("OG");
			else if (Dstr.charAt(i) == '2')
				resultString = resultString.concat("AL");
			else if (Dstr.charAt(i) == '3')
				resultString = resultString.concat("RI");
			else if (Dstr.charAt(i) == '4')
				resultString = resultString.concat("RE");
			else if (Dstr.charAt(i) == '5')
				resultString = resultString.concat("SE");
			else if (Dstr.charAt(i) == '6')
				resultString = resultString.concat("AT");
			else if (Dstr.charAt(i) == '7')
				resultString = resultString.concat("UL");
			else if (Dstr.charAt(i) == '8')
				resultString = resultString.concat("IN");
			else if (Dstr.charAt(i) == '9')
				resultString = resultString.concat("NG");
		}

		return resultString;
	}

	public static void start(){
		con = DBHelper.getConnection();
		deleteTables();
		createTables();
		populateStringKeyTable();
		populateUnionKeyTable();
		DBHelper.closeConnection();
	}
	
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		start();
		long end = System.currentTimeMillis();
		DecimalFormat fnum = new DecimalFormat("##0.00");    
		String time=fnum.format((float)(end-start)/1000);       
		System.out.println("tables populated in : " + time + " s");
	}
}
