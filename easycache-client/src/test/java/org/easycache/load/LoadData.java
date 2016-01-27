package org.easycache.load;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

import org.easycache.jdbc.DBHelper;
import org.easycache.jdbc.IMDGHelper;

import com.easycache.sqlclient.HazelcastDatabaseMetaData;
import com.easycache.sqlclient.TableMetaData;
import com.easycache.sqlclient.load.Loader;
import com.easycache.sqlclient.load.LoaderHelper;

public class LoadData {

	private HazelcastDatabaseMetaData hazelcastDatabaseMetaData = new HazelcastDatabaseMetaData();
	private Set<String> rsHashSet = new HashSet<String>();

	static {
		Loader loader = new Loader();
		long start = System.currentTimeMillis();
		loader.loadData();
		long end = System.currentTimeMillis();
		DecimalFormat fnum = new DecimalFormat("##0.00");
		String time = fnum.format((float) (end - start) / 1000);
		System.out.println("load data time: " + time + " s");
	}

	public void loadDataValidation() throws Exception {
		Connection conn1 = IMDGHelper.getConnection();
		Connection conn2 = DBHelper.getConnection();
		for (String tableName : hazelcastDatabaseMetaData.getTableNames()) {
			int num = 0;
			String sql = "select * from " + tableName;
			PreparedStatement pst1 = conn1.prepareStatement(sql);
			ResultSet rsSet1 = pst1.executeQuery();
			TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
			while (rsSet1.next()) {
				String temp = "";
				for (int i = 0; i < tableMetaData.getColumnSize(); i++) {
					Object attributeValue = LoaderHelper.getAttributeValue(tableMetaData
							.getColumnClassByAttributeIndex(i).getName(), rsSet1, i + 1);
					if (attributeValue == null) {
						System.out.println("value null, tableName: " + tableName + " index: " + i
								+ " columnclassName: "
								+ tableMetaData.getColumnClassByAttributeIndex(i).getName());
						break;
					}
					temp = temp + " $#@ " + attributeValue.toString();
				}
				num++;
				rsHashSet.add(temp);
			}
			System.out.println("tableName: " + tableName + " rsHashSet size: " + rsHashSet.size()
					+ " num: " + num);
			PreparedStatement pst2 = conn2.prepareStatement(sql);
			ResultSet rsSet2 = pst2.executeQuery();
			while (rsSet2.next()) {
				String temp = "";
				for (int i = 0; i < tableMetaData.getColumnSize(); i++) {
					// String attributeName = tableMetaData.getColumnName(i);
					Object attributeValue = LoaderHelper.getAttributeValue(tableMetaData
							.getColumnClassByAttributeIndex(i).getName(), rsSet2, i + 1);
					if (attributeValue == null) {
						System.out.println("value null, tableName: " + tableName + " index: " + i
								+ " columnclassName: "
								+ tableMetaData.getColumnClassByAttributeIndex(i).getName());
						break;
					}
					temp = temp + " $#@ " + attributeValue.toString();
				}
				if (!rsHashSet.remove(temp)) {
					System.out.println(tableName + " loaded incorrectly:" + temp);
					break;
				}
			}
			if (rsHashSet.isEmpty()) {
				System.out.println(tableName + " loaded correctly.");
			} else {
				System.out.println(tableName + " loaded incorrectly.");
			}
			rsSet1.close();
			pst1.close();
			rsSet2.close();
			pst2.close();
		}
		conn1.close();
		conn2.close();
	}
	
	public static void main(String [] args){
		System.out.println("start......");
	}
}
