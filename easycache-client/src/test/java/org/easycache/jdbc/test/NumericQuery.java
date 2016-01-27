package org.easycache.jdbc.test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.easycache.jdbc.IMDGHelper;
import org.easycache.load.LoadData;

public class NumericQuery {
	
	public void imdgExecuteQuery() {
		try {
			String sql = "select * from NUMERIC_XX1 where id = ?";
			Connection connImdg = IMDGHelper.getConnection();
			PreparedStatement pstImdg = connImdg.prepareStatement(sql);
			pstImdg.setBigDecimal(1, new BigDecimal(1));
			ResultSet rsSetImdg = pstImdg.executeQuery();
			while (rsSetImdg.next()) {
				System.out.println(rsSetImdg.getBigDecimal(1));
				System.out.println(rsSetImdg.getString(2));
				System.out.println(rsSetImdg.getBigDecimal(3));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void test(){
		new LoadData();
		imdgExecuteQuery();
	}
	
	public static void main(String [] args) {
		new NumericQuery().test();
	}
}
