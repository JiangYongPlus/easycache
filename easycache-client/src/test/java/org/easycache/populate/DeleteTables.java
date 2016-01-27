package org.easycache.populate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import org.easycache.jdbc.DBHelper;

public class DeleteTables {
	private static Connection con = null;

	public static void deleteTables() {
		int i;
		String[] tables = { "ADDRESS", "AUTHOR", "CC_XACTS", "COUNTRY", "CUSTOMER", "ITEM",
				"ORDER_LINE", "ORDERS", "SHOPPING_CART", "SHOPPING_CART_LINE", "STRINGKEY", "UNIONKEY"};
//		String[] tables = { "ADDRESS", "STRINGKEY", "UNIONKEY" };
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
	

	public static void start(){
		con = DBHelper.getConnection();
		deleteTables();
		DBHelper.closeConnection();
	}
	
	public static void main(String[] args) {
		start();
	}
}
