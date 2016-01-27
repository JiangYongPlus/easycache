package com.easycache.sqlclient.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.easycache.sqlclient.type.DataType;
import com.easycache.sqlclient.type.DataTypeCheck;
import com.hazelcast.easycache.serialization.HazelcastObject;

public class IMDGResult extends HazelcastObject{
	/*
	 * 关闭标示
	 */
	protected boolean closed = false;
	/*
	 * 结果集行数与列数
	 */
	protected int resultCols = -1;
	protected int resultRows = -1;
	protected int resultCurrent = -1;
	protected Object lastRead = null;
	/*
	 * <列名称, 列索引> 索引号从1开始
	 */
	protected HashMap<String, Integer> colsNameMap = new HashMap<String, Integer>();
	/*
	 * 查询列类型信息
	 */
	protected ArrayList<DataType.Type> colsTypeList = new ArrayList<DataType.Type>();
	/*
	 * 查询列所属表名信息
	 */
	protected ArrayList<String> colsTabList = new ArrayList<String>();
	/*
	 * 查询列所属数据库名信息
	 */
	protected ArrayList<String> colsDBList = new ArrayList<String>();
	/*
	 * 查询结果值信息
	 */
	protected ArrayList<ArrayList<Object>> colsValuesList = new ArrayList<ArrayList<Object>>();

	@SuppressWarnings("rawtypes")
	public void addMetaDataCol(String columnName, Class columnClass, String tableName, String databaseName) {
		this.colsNameMap.put(columnName.toUpperCase(), this.colsNameMap.size() + 1);
		String typeName = columnClass.getName();
		this.colsTypeList.add(DataTypeCheck.check(typeName));
		this.colsTabList.add(tableName);
		this.colsDBList.add(databaseName);
	}
	/*
	 * 添加<列名, 列类型, 所属表名, 所属数据库名>信息
	 */
	public void addMetaData(List<String> namesList, List<String> classNameList, List<String> colsTabList, List<String> colsDBList) {
		List<DataType.Type> typeList = new ArrayList<DataType.Type>();
		int size = 0;
		if (namesList != null) {
			size = namesList.size();
			for (int i = 0; i < size; i++) {
				colsNameMap.put(namesList.get(i).toUpperCase(), i + 1);
			}
		}
		if (classNameList != null) {
			size = classNameList.size();
			for (int i = 0; i < size; i++) {
				String typeName = classNameList.get(i);
				typeList.add(DataTypeCheck.check(typeName));
			}
		}
		if (colsTabList == null || colsDBList == null) {
			size = typeList.size();
			for (int i = 0; i < size; i++) {
				this.colsTypeList.add(typeList.get(i));
				this.colsTabList.add(null);
				this.colsDBList.add(null);
			}

		} else {
			size = typeList.size();
			for (int i = 0; i < size; i++) {
				this.colsTypeList.add(typeList.get(i));
				this.colsTabList.add(colsTabList.get(i));
				this.colsDBList.add(colsDBList.get(i));
			}
		}
		// for (int i = 0; i < typeList.size(); i++) {
		// this.colsTypeList.add(typeList.get(i));
		// // 目前先不写会table名和database名
		// //this.colsTabList.add(colsTabList.get(i));
		// //this.colsDBList.add(colsDBList.get(i));
		// }
	}

	/*
	 * 按列添加检索值信息
	 */
	public void addValueData(List<List<Object>> objlist) throws SQLException {
		if (objlist == null) {
			return;
		}
		int cols = objlist.size();
		int rows = objlist.get(0).size();
		for (int i = 0; i < rows; i++) {
			colsValuesList.add(new ArrayList<Object>());
		}
		for (int i = 0; i < cols; i++) {
			for (int j = 0; j < rows; j++) {
				colsValuesList.get(j).add(objlist.get(i).get(j));
			}
		}
	}

	/*
	 * 一次添加一列
	 */
	public void addValueDataColumn(List<Object> objlist) throws SQLException {
		if (objlist == null) {
			return;
		}
		int rows = objlist.size();
		if (colsValuesList.size() == 0) {
			for (int i = 0; i < rows; i++) {
				colsValuesList.add(new ArrayList<Object>());
			}
		}
		for (int i = 0; i < rows; i++) {
			colsValuesList.get(i).add(objlist.get(i));
		}
	}

	/*
	 * 按行添加值
	 */
	public void addRowValueData(List<List<Object>> objlist) throws SQLException {
		if (objlist == null) {
			return;
		}
		for (List<Object> e : objlist) {
			ArrayList<Object> list = new ArrayList<Object>();
			for (Object obj : e) {
				list.add(obj);
			}
			colsValuesList.add(list);
		}
	}

	/*
	 * Insert primary key return value: ResultSet
	 */
	@SuppressWarnings("rawtypes")
	public void addPrimaryKey(Object key, Class keyclass) {
		this.colsNameMap.put("GENERATED_KEY", 1);
		String typeName = keyclass.getName();
		this.colsTypeList.add(DataTypeCheck.check(typeName));
		this.colsTabList.add(null);
		this.colsDBList.add(null);
		if (this.colsValuesList.size() == 0) {
			colsValuesList.add(new ArrayList<Object>());
			colsValuesList.get(0).add(key);
		}
	}

	/*
	 * 确认信息
	 */
	protected void checkResult() throws SQLException {
		resultRows = colsValuesList.size();
		resultCols = colsTypeList.size();
		for (ArrayList<Object> list : colsValuesList) {
			if (resultCols != list.size()) {
				ErrorDump();
				throw new SQLException(
						"resultCols is not  equal to list.size()\n");
			}
		}
		if (resultCols != colsNameMap.size()) {
			ErrorDump();
			throw new SQLException(
					"resultCols is not equal to colsNameMap.size()");
		}
		closed = false;
	}

	/*
	 * 清理数据
	 */
	public void clear() {
		colsNameMap.clear();
		colsTypeList.clear();
		colsTabList.clear();
		colsDBList.clear();
		colsValuesList.clear();

		resultCols = -1;
		resultRows = -1;
		resultCurrent = -1;
		lastRead = null;
	}

	/*
	 * error dump
	 */
	protected void ErrorDump() {
		System.out.println("==================Meta Info====================");
		System.out.println("resultCols:" + resultCols + "\nresultRows:"
				+ resultRows + "\nresultCurrent:" + resultCurrent);

		LinkedList<Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(
				colsNameMap.entrySet());
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				Comparable<Integer> v1 = o1.getValue();
				Integer v2 = o2.getValue();
				if (v1 == null) {
					if (v2 == null) {
						return 0;
					} else {
						return -1;
					}
				} else {
					if (v2 == null) {
						return 1;
					} else {
						return v1.compareTo(v2);
					}
				}
			}
		});
		Iterator<Entry<String, Integer>> iterator = list.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			Entry<String, Integer> entry = iterator.next();
			System.out.println(entry.getKey() + " : " + entry.getValue()
					+ " : " + colsTypeList.get(i) + " : "
					+ colsTabList.get(i++));
		}
		System.out
				.println("~~~~~~~~~~~~~~~~~~~~Value Info~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("value row number = " + colsValuesList.size());
		for (ArrayList<Object> objlist : colsValuesList) {
			System.out.println("value column size = " + objlist.size());
			i = 0;
			for (Object object : objlist) {
				System.out.println(++i + ": " + object);
			}
		}
	}
}
