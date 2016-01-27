package com.easycache.sqlclient.load;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.sf.cglib.beans.BeanGenerator;
import net.sf.cglib.beans.BeanMap;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.easycache.persistence.HazelcastObjectMapStore;
import com.hazelcast.easycache.serialization.HazelcastObject;
import com.hazelcast.easycache.utility.ConnectionPool;
import com.hazelcast.easycache.utility.IMDGString;
import com.easycache.sqlclient.distributed.DistributedIMDGHelper;
import com.easycache.sqlclient.BeanGeneratorFactory;
import com.easycache.sqlclient.HazelcastDatabaseMetaData;
import com.easycache.sqlclient.TableMetaData;
import com.easycache.sqlclient.jdbc.IMDGDriver;
import com.easycache.sqlclient.type.DataTypeCheck;
import com.easycache.sqlclient.utility.ThreadPool;

public class Loader {

	private BeanGeneratorFactory beanGeneratorFactory = new BeanGeneratorFactory();
	private HazelcastDatabaseMetaData hazelcastDatabaseMetaData = new HazelcastDatabaseMetaData();
	private HazelcastInstance hazelcast = null;
	private boolean hasLoad = false;
	private static String dbType = null;
	private static boolean loadConcurrently = false;
	private static int pageSize;
	private static Object lock_beanGenerator = new Object();

	private static String schema = null;
	private static Set<String> loadTablesSet = new HashSet<String>();
	private static Set<String> unsupportedTableSet = new HashSet<String>();
	private static boolean loadPartially = false;

	private DistributedIMDGHelper distIMDGHelper = new DistributedIMDGHelper("IMDG");
	private static int masterAlarmClock = 2;// after masterAlarmClock, the master
										// node will set the metadata state
										// ready
	private static String onlySlaveLoad;

	public Loader() {
		if ((hazelcast = Hazelcast.getHazelcastInstanceByName("IMDG")) == null) {
			Config config = new XmlConfigBuilder().build();
			config.setInstanceName("IMDG");
			hazelcast = Hazelcast.newHazelcastInstance(config);
		}
		// assumption that all of the other nodes connected to database already,
		// before distributed data loading
		// zhaohui liu, for distributed loading, 2015-3-11
		distIMDGHelper.raceToMaster();
	}

	public static void setLoadTables(String[] LoadTables) {
		if (LoadTables != null) {
			for (String tableName : LoadTables) {
				loadTablesSet.add(tableName);
			}
		}
	}

	public static void setLoadPartially(boolean loadPartially) {
		Loader.loadPartially = loadPartially;
	}

	public static void setThreadParam(boolean loadConcurrently, int pageSize, int maxThreadPoolSize) {

		Loader.loadConcurrently = loadConcurrently;
		Loader.pageSize = pageSize;
		// Loader.maxThreadPoolSize = maxThreadPoolSize;
	}

	public static String getDBType() {
		if (dbType != null) {
			return dbType;
		} else {
			try {
				throw new Exception("warning： dbType is not initialized!");
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	public static boolean isSupportedTable(String tableName) {
		if (loadPartially == true && !loadTablesSet.contains(tableName)) {
			return false;
		}
		if (unsupportedTableSet.contains(tableName)) {
			return false;
		}
		return true;
	}

	public Connection getConnection() {
		return ConnectionPool.getConnection();
	}

	public void loadMetaData() {
		try {
			Connection con = this.getConnection();
			Statement statement = con.createStatement();
			DatabaseMetaData dbMetaData = con.getMetaData();
			// get MetaData about all tables
			ResultSet resultSet = null;
			if (dbType.equals("oracle") || dbType.equals("shentong")) {
				resultSet = dbMetaData.getTables(null, schema, null, new String[] { "TABLE" });
			} else if (dbType.equals("mysql")) {
				resultSet = dbMetaData.getTables(null, null, null, new String[] { "TABLE" });
			} else {
				throw new Exception("Warning: dbType error: " + dbType);
			}
			while (resultSet.next()) {
				String tableName = resultSet.getString("TABLE_NAME").toLowerCase();
				if (loadPartially == true && !loadTablesSet.contains(tableName)) {
					continue;
				}
				String sql = null;
				if (dbType.equals("oracle") || dbType.equals("shentong")) {
					sql = "select * from " + tableName + " WHERE rownum < 2";
				} else if (dbType.equals("mysql")) {
					sql = "select * from " + tableName + " limit 1";
				} else {
					throw new Exception("Warning: dbType error: " + dbType);
				}
				ResultSet rsSet = statement.executeQuery(sql);
				ResultSetMetaData rsData = rsSet.getMetaData();
				for (int i = 1; i <= rsData.getColumnCount(); i++) {
					String className = rsData.getColumnClassName(i);
					if (!LoaderHelper.isSupportedType(DataTypeCheck.check(className))) {
						unsupportedTableSet.add(tableName);
						break;
					}
				}
				if (unsupportedTableSet != null && unsupportedTableSet.contains(tableName)) {
					continue;
				}
				BeanGenerator beanGenerator = new BeanGenerator();
				beanGenerator.setNamingPolicy(new IMDGNamingPolicy(tableName));
				beanGenerator.setSuperclass(HazelcastObject.class);
				TableMetaData tableMetaData = new TableMetaData();
				tableMetaData.setTableName(tableName);
				// System.out.println("tableName: " + tableName);
				for (int i = 1; i <= rsData.getColumnCount(); i++) {

					String columnName = rsData.getColumnName(i).toLowerCase();
					String className = rsData.getColumnClassName(i);
					// System.out.println("columnIndex: " + i + "	columnName: "
					// + columnName + "	className: "
					// + className);
					beanGenerator.addProperty(columnName, LoaderHelper.getClassFromString(className));
					tableMetaData.addColumnName(columnName);
					tableMetaData.addColumnClass(LoaderHelper.getClassFromString(className));
				}
				rsSet.close();

				ResultSet rs = null;
				if (dbType.equals("shentong") || dbType.equals("oracle")) {
					rs = dbMetaData.getPrimaryKeys(null, schema, tableName.toUpperCase());
				} else if (dbType.equals("mysql")) {
					rs = dbMetaData.getPrimaryKeys(null, null, tableName);
				} else {
					throw new Exception("Warning: dbType error: " + dbType);
				}
				while (rs.next()) {
					/*
					 * String name = rs.getString("table_name");
					 * System.out.println("table name: " + name); String
					 * columnName = rs.getString("column_name");
					 * System.out.println("column name: " + columnName); String
					 * keySeq = rs.getString("key_seq");
					 * System.out.println("sequence in key: " + keySeq); String
					 * pkName = rs.getString("pk_name");
					 */
					String columnName = rs.getString("column_name").toLowerCase();
					tableMetaData.addPrimaryKey(columnName);
				}
				rs.close();
				beanGeneratorFactory.setBeanGenerator(tableName, beanGenerator);
				hazelcastDatabaseMetaData.setTableMetaData(tableName, tableMetaData);

				// zhaohui liu, spread information to other nodes
				distIMDGHelper.spreadMetaInfo(tableMetaData);
			}
			distIMDGHelper.setMasterMetaStat(true);
			resultSet.close();
			con.close();
		} catch (Exception e) {
			System.err.println("SQLException :" + e.getMessage());
			e.printStackTrace();
		}
		System.out.println("load metadata done!");
	}

	public void loadIndex() {
		try {
			Connection con = this.getConnection();
			DatabaseMetaData dbMetaData = con.getMetaData();
			ResultSet resultSet = null;
			if (dbType.equals("shentong") || dbType.equals("oracle")) {
				resultSet = dbMetaData.getTables(null, schema, null, new String[] { "TABLE" });
			} else if (dbType.equals("mysql")) {
				resultSet = dbMetaData.getTables(null, null, null, new String[] { "TABLE" });
			} else {
				throw new Exception("Warning: dbType error: " + dbType);
			}
			System.out.println("creating index from backend database...");
			while (resultSet.next()) {
				String tableName = resultSet.getString("TABLE_NAME").toLowerCase();
				if (!isSupportedTable(tableName)) {
					continue;
				}
				IMap<String, Object> map = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
				ResultSet indexInfo = null;
				if (dbType.equals("oracle")) {
					indexInfo = dbMetaData.getIndexInfo(null, schema, tableName.toUpperCase(), false, false);
					while (indexInfo.next()) {
						short type = indexInfo.getShort("TYPE");
						if (type != DatabaseMetaData.tableIndexStatistic) {
							String dbIndexName = indexInfo.getString("INDEX_NAME");
							String dbColumnName = indexInfo.getString("COLUMN_NAME");
							if (!dbIndexName.startsWith("SYS")) {
								map.addIndex(dbColumnName.toLowerCase(), true);
							}
						}
					}
				} else if (dbType.equals("mysql")) {
					indexInfo = dbMetaData.getIndexInfo(null, null, tableName, false, false);
					while (indexInfo.next()) {
						short type = indexInfo.getShort("TYPE");
						if (type != DatabaseMetaData.tableIndexStatistic) {
							String dbIndexName = indexInfo.getString("INDEX_NAME");
							String dbColumnName = indexInfo.getString("COLUMN_NAME");
							if (!dbIndexName.equals("PRIMARY")) {
								map.addIndex(dbColumnName.toLowerCase(), true);
							}
						}
					}
				} else if (dbType.equals("shentong")) {
					indexInfo = dbMetaData.getIndexInfo(null, schema, tableName.toUpperCase(), false, false);
					while (indexInfo.next()) {
						short type = indexInfo.getShort("TYPE");
						if (type != DatabaseMetaData.tableIndexStatistic) {
							String dbIndexName = indexInfo.getString("INDEX_NAME");
							String dbColumnName = indexInfo.getString("COLUMN_NAME");
							// System.out.println("tableName:"+tableName);
							// System.out.println("indexName:"+dbIndexName);
							// System.out.println("columnName:"+dbColumnName);
							if (!dbIndexName.endsWith("PKEY")) {
								map.addIndex(dbColumnName.toLowerCase(), true);
							}
						}
					}
				} else {
					throw new Exception("Warning: dbType error: " + dbType);
				}
			}
			System.out.println("index created.");
			resultSet.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createIndex() {
		try {
			Properties prop = new Properties();
			InputStream in = Loader.class.getResourceAsStream(ConfigParser.getConfigFilePath());
			if (in != null) {
				prop.load(in);
				String indexSwitch = prop.getProperty("indexSwitch");
				if (indexSwitch.equals("0")) {
					this.loadIndex();
				} else if (indexSwitch.equals("1")) {
					System.out.println("creating index...");
					String indexTables = prop.getProperty("indexTables");
					for (String indexTable : ConfigParser.stringSplit(indexTables)) {
						if (!isSupportedTable(indexTable.toLowerCase())) {
							continue;
						}
						IMap<String, Object> map = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX
								+ indexTable.toLowerCase());
						String indexTableColumns = prop.getProperty(indexTable);
						for (String indexTableColumn : ConfigParser.stringSplit(indexTableColumns)) {
							map.addIndex(indexTableColumn.toLowerCase(), true);
						}
					}
					System.out.println("index created!");
				} else if (indexSwitch.equals("2")) {
					System.out.println("no index created!");
				} else {
					throw new IOException("indexSwitch error, no index created!");
				}
			} else {
				throw new IOException("warning: can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadData() {
		// jiang yong, for load data, insert
		HazelcastObjectMapStore.setEnalbe(false);
		ConfigParser.loaderConfigParse();
		ConfigParser.executorConfigParse();
		ConfigParser.queryResultCacheConfigParse();
		ConfigParser.pstOptimizationConfigParse();
		ConfigParser.loadPartiallyConfigParse();
		ConfigParser.localOptimizationConfigParse();
		dbType = ConfigParser.getInstance().configParseGetDbType();
		schema = ConfigParser.getInstance().configParseGetSchema();
		if (!DistributedIMDGHelper.isSlaveNode()) {// master node
			this.loadMetaData();
			this.generateClass();
			// statistics of database table infomations of pages
			this.getDatabaseTableInfo();
			// statistics of imdg node members
			try {
				System.out.println("Master Node will sleep " + masterAlarmClock
						+ "s to wait other nodes!<--");
				Thread.sleep(masterAlarmClock * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			distIMDGHelper.initLoadPart();
			distIMDGHelper.setAllGeneClassStat(true);
			System.out.println("Master Node wake up all the nodes to load data!<--");
		} else {// slave nodes
			try {
				while (!distIMDGHelper.isMasterMetaReady()) {
					System.out.println("Slave Node wait to load metadata!<--");
					Thread.sleep(masterAlarmClock * 100);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Slave Node start to localize the metadata !");
			distIMDGHelper.localizeMetaInfo(beanGeneratorFactory, hazelcastDatabaseMetaData);
			this.generateClass();

			try {
				while (!distIMDGHelper.isAllGeneClassReady()) {
					System.out
							.println("Slave Node wait for others to generate dynamic class!<--");
					Thread.sleep(masterAlarmClock * 100);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		this.createIndex();
		int nodesNum = distIMDGHelper.sizeOfIMDG();

		if (loadConcurrently == true) {
			if (nodesNum > 1) {
				System.out.println("distributed homogenization data loading...");
				loadDataDistributed();
			} else {
				loadDataConcurrently();
			}
		} else {
			loadDataSingly();
		}
		IMDGDriver.loadflag = true;
		// jiang yong, for persistence, update, delete and insert
		HazelcastObjectMapStore.setEnalbe(true);
	}

	public void generateClass() {
		for (String tableName : hazelcastDatabaseMetaData.getTableNames()) {
			TableInfo tableInfo = new TableInfo(tableName);
			tableInfo.setMaxKey(0);

			BeanGenerator beanGenerator = beanGeneratorFactory.getBeanGenerator(tableName);
			if (beanGenerator == null) {
				try {
					throw new SQLException("no such table");
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			IMap<String, Object> map = hazelcast.getMap(tableName);
			TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
			@SuppressWarnings("rawtypes")
			Class hazelcastObjectClass;
			HazelcastObject hazelcastObject;
			synchronized (lock_beanGenerator) {
				hazelcastObjectClass = (Class) beanGenerator.createClass();
				hazelcastObject = (HazelcastObject) beanGenerator.create();
			}
			BeanMap beanMap = BeanMap.create(hazelcastObject);
			IdGenerator idGenerator = hazelcast.getIdGenerator(tableName);
		}
	}

	/**
	 * statistics of database table infomations of pages, for page number
	 * list(2,1,2,4,9)  translate to accumulated (2,3,5,9,18)
	 */
	public void getDatabaseTableInfo() {
		// changed by zhaohui liu , 2015-3-18
		Connection con = this.getConnection();
		IList<Integer> accuTablePage = hazelcast.getList(IMDGString.ACCUT_TABLE_PAGE);
		Integer curPageNum = new Integer(0);
		// accuTablePage.add(curPageNum);
		IMap<Integer, String> numToNameMap = hazelcast.getMap(IMDGString.NODE_NUM_TO_TABLE_NAME);
		try {
			Integer tableNum = 0;
			for (String tableName : hazelcastDatabaseMetaData.getTableNames()) {
				numToNameMap.put(tableNum, tableName);

				Statement statementCount = con.createStatement();
				String sqlCount = "select count(*) as tableSize from " + tableName;
				ResultSet rsSetCount = statementCount.executeQuery(sqlCount);

				rsSetCount.next();
				long tableSize = rsSetCount.getLong("tableSize");
				rsSetCount.close();

				long pageNum;
				if (tableSize % pageSize == 0) {
					pageNum = tableSize / pageSize;
				} else {
					pageNum = tableSize / pageSize + 1;
				}

				// TODO zhaohui liu 2015-3-18, maybe a little risk for corner
				// case, such as 1000000000000...
				curPageNum = (int) (curPageNum + pageNum);
				accuTablePage.add(curPageNum);
				// --------------------------------------------------------------//
//				System.out.println("tableNum : " + tableNum + "\t\t" + tableName);
//				System.out.println("tableSize : " + tableSize + "\t\t pageNum : " + pageNum
//						+ "\t\tcurPageNum : " + curPageNum);
				// --------------------------------------------------------------//
				tableNum = tableNum + 1;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * zhaohui liu, for distributed data loading
	 */
	public void loadDataDistributed() {

		try {
			//
			Connection con = this.getConnection();
			int curNodeNum = DistributedIMDGHelper.getCurNodeNum();
			int loadNodeNum = distIMDGHelper.sizeOfIMDG();
			System.out.println("Node start to load data, number of node is : " + loadNodeNum);

			IList<Integer> accuTablePage = hazelcast.getList(IMDGString.ACCUT_TABLE_PAGE);
			int totalPageNum = accuTablePage.get(accuTablePage.size() - 1);
			// zhaohui liu , 2015-3-19, only slave node will lead more average
			// dataloading
			if (onlySlaveLoad.equals("true")) {
				System.out.println("Only slave node start to load data");
				if (!DistributedIMDGHelper.isSlaveNode()) {
					return;
				} else {
					loadNodeNum--;
					curNodeNum--;
				}
			} else {
				System.out.println("All of the node will load data");
			}

			int nPageForNode = (totalPageNum - 1) / loadNodeNum + 1;

			// [iStart, iEnd)
			int iStart = curNodeNum * nPageForNode;
			int iEnd = (curNodeNum + 1) * nPageForNode;
			iEnd = iEnd < totalPageNum ? iEnd : totalPageNum;

			IMap<Integer, String> numToNameMap = hazelcast.getMap(IMDGString.NODE_NUM_TO_TABLE_NAME);

			Iterator<Integer> it = accuTablePage.iterator();
			int lastAccuNum = -1;
			int curAccuNum = 0;
			Integer curTableNum = -1;
			String tableName = null;

			ExecutorService executor = ThreadPool.getInstance().getExecutor();
			while (iStart < iEnd) {
				while (it.hasNext() && iStart >= curAccuNum) {
					lastAccuNum = curAccuNum;
					curAccuNum = it.next();
					curTableNum++;
				}
				tableName = numToNameMap.get(curTableNum);
				int iOffset = iStart - lastAccuNum;
				long offset = iOffset * pageSize;
				// -----------------------------------------------------------------------------//
				// System.out.println("------------------------------loadDataDistributed-------------------------------");
				// System.out.println("load Node Num : " + loadNodeNum +
				// "\t\tcurNodeNum : " + curNodeNum);
				// System.out.println("totalPageNum : " + totalPageNum +
				// "\t\tnPageForNode : " + nPageForNode);
				// System.out.println("iStart : " + iStart + "\t\tiEnd : " +
				// iEnd);
				// System.out.println("tableName : " + tableName);
				// System.out.println("iOffset + " + iOffset + "\t\tpageSize + "
				// + pageSize);
				// System.out.println("offset + " + offset);
				// System.out.println("------------------------------loadDataDistributed-------------------------------");
				// -----------------------------------------------------------------------------//
				iStart++;

				TableInfo tableInfo = new TableInfo(tableName);
				tableInfo.setMaxKey(0);

				IdGenerator idGenerator = hazelcast.getIdGenerator(tableName);
				BeanGenerator beanGenerator = beanGeneratorFactory.getBeanGenerator(tableName);
				if (beanGenerator == null) {
					throw new SQLException("no such table");
				}

				IMap<String, Object> map = hazelcast.getMap(tableName);
				Runnable loadTableSubThread = new LoadTableSubTask(tableInfo, con, offset, beanGenerator, map);
				executor.execute(loadTableSubThread);
				idGenerator.init(tableInfo.getMaxKey());
			}
			executor.shutdown();
			while (!executor.isTerminated()) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			con.close();
			System.out.println("This node have loaded data completely !");
		} catch (Exception e) {
			System.err.println("SQLException :" + e.getMessage());
			e.printStackTrace();
		}
	}

	public void loadDataSingly() {
		System.out.println("begin to load data......");
		try {
			Connection con = this.getConnection();
			Statement statement = con.createStatement();
			for (String tableName : hazelcastDatabaseMetaData.getTableNames()) {
				// System.out.println("tableName is:" + tableName);
				String sql = "select * from " + tableName;
				ResultSet rsSet = statement.executeQuery(sql);
				TableInfo tableInfo = new TableInfo(tableName);
				tableInfo.setMaxKey(0);
				BeanGenerator beanGenerator = beanGeneratorFactory.getBeanGenerator(tableName);
				if (beanGenerator == null) {
					throw new SQLException("no such table");
				}
				// IMap<String, Object> map = hazelcastClient.getMap(tableName);
				IMap<String, Object> map = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
				map = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
				TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
				@SuppressWarnings("rawtypes")
				Class hazelcastObjectClass = (Class) beanGenerator.createClass();
//				Object object = beanGenerator.create();
				// System.out.println(object.getClass().getName());
				HazelcastObject hazelcastObject = (HazelcastObject) beanGenerator.create();
				// System.out.println(hazelcastObject.getClass().getName());
				BeanMap beanMap = BeanMap.create(hazelcastObject);
				IdGenerator idGenerator = hazelcast.getIdGenerator(tableName);
				String strid = "";
				if (hasLoad) {
					continue;
				}
				while (rsSet.next()) {
					for (int i = 0; i < tableMetaData.getColumnSize(); i++) {
						String attributeName = tableMetaData.getColumnName(i);
						// System.out.println("tableName is: " + tableName);
						// System.out.println("attributeName is: " +
						// attributeName);
						Object attributeValue = LoaderHelper.getAttributeValue(tableMetaData
								.getColumnClassByAttributeIndex(i).getName(), rsSet, i + 1);
						try {
							beanMap.put(attributeName, attributeValue);
						} catch (Exception e) {
							e.printStackTrace();
							System.out.println(tableMetaData.getColumnClassByAttributeIndex(i).getName());
							System.out.println(tableName);
							System.out.println(attributeName);
						}
					}
					// jiang yong 2015-06-14
					// the primaryKey is like "tableName:--:key=kev_val",
					// tableName is for persistence
					if (tableMetaData.getPrimaryKeyListSize() == 1) {
						strid = tableMetaData.getPrimaryKeyList().get(0);
						String className = tableMetaData.getColumnClassByAttributeName(strid).getName();
						if (className.equalsIgnoreCase("java.lang.Integer")) {
							int key = rsSet.getInt(strid);
							if (key > tableInfo.getMaxKey()) {
								tableInfo.setMaxKey(key);
							}
							strid = tableName + IMDGString.TABLE_TAG + strid + "=" + String.valueOf(key);
						} else if (!className.equalsIgnoreCase("java.lang.String")) {
							Object key = LoaderHelper.getAttributeValue(className, rsSet, strid);
							strid = tableName + IMDGString.TABLE_TAG + strid + "=" + String.valueOf(key);
						} else {
							Object key = LoaderHelper.getAttributeValue(className, rsSet, strid);
							strid = tableName + IMDGString.TABLE_TAG + strid + "=" + "'"
									+ String.valueOf(key) + "'";
						}
					} else {
						strid = tableName + IMDGString.TABLE_TAG;
						for (String primaryAttributeName : tableMetaData.getPrimaryKeyList()) {
							strid += primaryAttributeName + "=" + beanMap.get(primaryAttributeName) + "$#@";
						}
						strid = strid.toLowerCase();
					}
					hazelcastObject.setId(strid);
					map.put(strid, hazelcastObject);
					hazelcastObject = (HazelcastObject) hazelcastObjectClass.newInstance();
					beanMap.setBean(hazelcastObject);
				}
				if (tableInfo.getMaxKey() == 0) {
					idGenerator.newId();
				} else {
					idGenerator.init(tableInfo.getMaxKey());
				}
				rsSet.close();
				System.out.println("load data from " + tableName + " done!");
			}
			con.close();
		} catch (Exception e) {
			System.err.println("SQLException :" + e.getMessage());
			e.printStackTrace();
		}
	}

	public void loadDataConcurrently() {
		System.out.println("begin to load data concurrently......");
		try {
			Connection con = this.getConnection();
			ExecutorService executor = ThreadPool.getInstance().getExecutor();
			ArrayList<Future<?>> futureList = new ArrayList<Future<?>>();
			for (String tableName : hazelcastDatabaseMetaData.getTableNames()) {
				Runnable loadDataSubTask = new LoadDataSubTask(tableName, con);
				futureList.add(executor.submit(loadDataSubTask));
			}
			for (int j = 0; j < futureList.size(); j++) {
				try {
					futureList.get(j).get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			// executor.shutdown();
			// while (!executor.isTerminated()) {
			// try {
			// Thread.sleep(10);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			// }
			con.close();
		} catch (Exception e) {
			System.err.println("SQLException :" + e.getMessage());
			e.printStackTrace();
		}
	}

	// jiang yong 2014-10-10 load tables concurrently
	class LoadDataSubTask implements Runnable {

		private String tableName = null;
		private Connection con = null;

		public LoadDataSubTask(String tableName, Connection con) {
			this.tableName = tableName;
			this.con = con;
		}

		public void run() {
			try {
				Statement statementCount = con.createStatement();
				String sqlCount = "select count(*) as tableSize from " + tableName;
				ResultSet rsSetCount = statementCount.executeQuery(sqlCount);
				rsSetCount.next();
				long tableSize = rsSetCount.getLong("tableSize");
				rsSetCount.close();
				// System.out.println(tableName + ":" + tableSize);
				TableInfo tableInfo = new TableInfo(tableName);
				tableInfo.setMaxKey(0);
				IdGenerator idGenerator = hazelcast.getIdGenerator(tableName);
				BeanGenerator beanGenerator = beanGeneratorFactory.getBeanGenerator(tableName);
				if (beanGenerator == null) {
					throw new SQLException("no such table");
				}
				// IMap<String, Object> map = hazelcastClient.getMap(tableName);
				IMap<String, Object> map = hazelcast.getMap(IMDGString.TABLE_NAME_PREFIX + tableName);
				TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
				@SuppressWarnings("rawtypes")
				Class hazelcastObjectClass = (Class) beanGenerator.createClass();
				HazelcastObject hazelcastObject = (HazelcastObject) beanGenerator.create();
				BeanMap beanMap = BeanMap.create(hazelcastObject);
				String strid = "";
				// if(hasLoad){continue;}
				if (!hasLoad) {
					if (tableSize > pageSize) {
						// System.out.println("start to load " + tableName
						// +" table concurrently...");
						long pageNum;
						if (tableSize % pageSize == 0) {
							pageNum = tableSize / pageSize;
						} else {
							pageNum = tableSize / pageSize + 1;
						}
						long offset = 0;
						ExecutorService executor = ThreadPool.getInstance().getExecutor();
						ArrayList<Future<?>> futureList = new ArrayList<Future<?>>();
						for (int i = 0; i < pageNum; i++) {
							offset = i * pageSize;
							Runnable loadTableSubThread = new LoadTableSubTask(tableInfo, con, offset,
									beanGenerator, map);
							futureList.add(executor.submit(loadTableSubThread));
							// executor.execute(loadTableSubThread);
						}
						for (int j = 0; j < futureList.size(); j++) {
							try {
								futureList.get(j).get();
							} catch (InterruptedException e) {
								e.printStackTrace();
							} catch (ExecutionException e) {
								e.printStackTrace();
							}
						}
						// executor.shutdown();
						// while (!executor.isTerminated()) {
						// try {
						// Thread.sleep(10);
						// } catch (InterruptedException e) {
						// e.printStackTrace();
						// }
						// }
					} else {
						Statement statement = con.createStatement();
						String sql = "select * from " + tableName;
						ResultSet rsSet = statement.executeQuery(sql);
						while (rsSet.next()) {
							for (int i = 0; i < tableMetaData.getColumnSize(); i++) {
								String attributeName = tableMetaData.getColumnName(i);
								Object attributeValue = LoaderHelper.getAttributeValue(tableMetaData
										.getColumnClassByAttributeIndex(i).getName(), rsSet, i + 1);
								try {
									beanMap.put(attributeName, attributeValue);
								} catch (Exception e) {
									e.printStackTrace();
									System.out.println(tableName);
									System.out.println(attributeName);
								}
							}
							if (tableMetaData.getPrimaryKeyListSize() == 1) {
								strid = tableMetaData.getPrimaryKeyList().get(0);
								String className = tableMetaData.getColumnClassByAttributeName(strid)
										.getName();
								if (className.equalsIgnoreCase("java.lang.Integer")) {
									int key = rsSet.getInt(strid);
									if (key > tableInfo.getMaxKey()) {
										tableInfo.setMaxKey(key);
									}
									strid = tableName + IMDGString.TABLE_TAG + strid + "="
											+ String.valueOf(key);
								} else if (!className.equalsIgnoreCase("java.lang.String")) {
									Object key = LoaderHelper.getAttributeValue(className, rsSet, strid);
									strid = tableName + IMDGString.TABLE_TAG + strid + "="
											+ String.valueOf(key);
								} else {
									Object key = LoaderHelper.getAttributeValue(className, rsSet, strid);
									strid = tableName + IMDGString.TABLE_TAG + strid + "=" + "'"
											+ String.valueOf(key) + "'";
								}
							} else {
								strid = tableName + IMDGString.TABLE_TAG;
								for (String primaryAttributeName : tableMetaData.getPrimaryKeyList()) {
									strid += primaryAttributeName + "=" + beanMap.get(primaryAttributeName)
											+ "$#@";
								}
								strid = strid.toLowerCase();
							}
							hazelcastObject.setId(strid);
							map.put(strid, hazelcastObject);
							hazelcastObject = (HazelcastObject) hazelcastObjectClass.newInstance();
							beanMap.setBean(hazelcastObject);
						}
						rsSet.close();
					}
					if (tableInfo.getMaxKey() == 0) {
						idGenerator.newId();
					} else {
						idGenerator.init(tableInfo.getMaxKey());
					}
					System.out.println("load data from " + tableName + " done!");
				}
			} catch (Exception e) {
				System.err.println("SQLException :" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	// jiang yong 2014-10-10 load one table concurrently
	class LoadTableSubTask implements Runnable {
		private Connection con;
		private long offset;
		private IMap<String, Object> map;
		private TableInfo tableInfo;
		private BeanGenerator beanGenerator;

		public LoadTableSubTask(TableInfo tableInfo, Connection con, long offset,
				BeanGenerator beanGenerator, IMap<String, Object> map) {
			this.tableInfo = tableInfo;
			this.con = con;
			this.offset = offset;
			this.map = map;
			this.beanGenerator = beanGenerator;

		}

		@SuppressWarnings("rawtypes")
		public void run() {
			try {
				String tableName = tableInfo.getTableName();
				TableMetaData tableMetaData = hazelcastDatabaseMetaData.getTableMetaData(tableName);
				Class hazelcastObjectClass;
				HazelcastObject hazelcastObject;
				synchronized (lock_beanGenerator) {
					hazelcastObjectClass = (Class) beanGenerator.createClass();
					hazelcastObject = (HazelcastObject) beanGenerator.create();
				}
				BeanMap beanMap = BeanMap.create(hazelcastObject);
				String strid = "";
				Statement statement = con.createStatement();
				String sql = null;
				if (dbType.equals("oracle") || dbType.equals("shentong")) {
					// sql = "select * from " + tableName + " WHERE rownum < " +
					// (offset + pageSize + 1)
					// + " minus select * from " + tableName +
					// " WHERE rownum < " + (offset + 1);
					sql = "select * from ( select *, rownum rnum from " + tableName + " where rownum < "
							+ (offset + pageSize + 1) + " ) where rnum > " + offset;
				} else if (dbType.equals("mysql")) {
					sql = "select * from " + tableName + " limit " + offset + "," + pageSize;
				} else {
					throw new Exception("Warning: dbType error: " + dbType);
				}
				// System.out.println("sql:" + sql);
				ResultSet rsSet = statement.executeQuery(sql);
				while (rsSet.next()) {
					for (int i = 0; i < tableMetaData.getColumnSize(); i++) {
						String attributeName = tableMetaData.getColumnName(i);
						Object attributeValue = LoaderHelper.getAttributeValue(tableMetaData
								.getColumnClassByAttributeIndex(i).getName(), rsSet, i + 1);
						try {
							beanMap.put(attributeName, attributeValue);
						} catch (Exception e) {
							e.printStackTrace();
							System.out.println(tableName);
							System.out.println(attributeName);
						}
					}
					if (tableMetaData.getPrimaryKeyListSize() == 1) {
						strid = tableMetaData.getPrimaryKeyList().get(0);
						String className = tableMetaData.getColumnClassByAttributeName(strid).getName();
						if (className.equalsIgnoreCase("java.lang.Integer")) {
							int key = rsSet.getInt(strid);
							if (key > tableInfo.getMaxKey()) {
								tableInfo.setMaxKey(key);
							}
							strid = tableName + IMDGString.TABLE_TAG + strid + "=" + String.valueOf(key);
						} else if (!className.equalsIgnoreCase("java.lang.String")) {
							Object key = LoaderHelper.getAttributeValue(className, rsSet, strid);
							strid = tableName + IMDGString.TABLE_TAG + strid + "=" + String.valueOf(key);
						} else {
							Object key = LoaderHelper.getAttributeValue(className, rsSet, strid);
							strid = tableName + IMDGString.TABLE_TAG + strid + "=" + "'"
									+ String.valueOf(key) + "'";
						}
					} else {
						strid = tableName + IMDGString.TABLE_TAG;
						for (String primaryAttributeName : tableMetaData.getPrimaryKeyList()) {
							strid += primaryAttributeName + "=" + beanMap.get(primaryAttributeName) + "$#@";
						}
						strid = strid.toLowerCase();
					}
					hazelcastObject.setId(strid);
					map.put(strid, hazelcastObject);
					hazelcastObject = (HazelcastObject) hazelcastObjectClass.newInstance();
					beanMap.setBean(hazelcastObject);
				}
				rsSet.close();
			} catch (Exception e) {
				System.err.println("SQLException :" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public void printMetaData() {
		try {
			Connection con = this.getConnection();
			DatabaseMetaData dbMetaData = con.getMetaData();
			System.out.println("URL:" + dbMetaData.getURL() + ";");
			System.out.println("UserName:" + dbMetaData.getUserName() + ";");
			System.out.println("isReadOnly:" + dbMetaData.isReadOnly() + ";");
			System.out.println("DatabaseProductName:" + dbMetaData.getDatabaseProductName() + ";");
			System.out.println("DatabaseProductVersion:" + dbMetaData.getDatabaseProductVersion() + ";");
			System.out.println("DriverName:" + dbMetaData.getDriverName() + ";");
			System.out.println("DriverVersion:" + dbMetaData.getDriverVersion());
			System.out.println("database product name: " + dbMetaData.getDatabaseProductName());
			System.out.println("whether support transaction: " + dbMetaData.supportsTransactions());
			System.out.println("version number of database: " + dbMetaData.getDatabaseProductVersion());
			System.out.println("isolation level of transaction: "
					+ dbMetaData.getDefaultTransactionIsolation());
			System.out.println("whether support batch updates: " + dbMetaData.supportsBatchUpdates());
			System.out.println("database url: " + dbMetaData.getURL());
			System.out.println("user name of database: " + dbMetaData.getUserName());
			System.out.println("whether read only model: " + dbMetaData.isReadOnly());
			System.out.println("whether support alias for column: " + dbMetaData.supportsColumnAliasing());
			System.out.println("whether support like: " + dbMetaData.supportsLikeEscapeClause());
			System.out.println("whether support limited outerjoins: "
					+ dbMetaData.supportsLimitedOuterJoins());
			System.out.println("whether support multiple transactions: "
					+ dbMetaData.supportsMultipleTransactions());
			System.out.println("whether support subsqueries in exists:"
					+ dbMetaData.supportsSubqueriesInExists());
			System.out.println("whether support subqueries in in sentence: "
					+ dbMetaData.supportsSubqueriesInIns());
			System.out.println("whether support given isolation level: "
					+ dbMetaData.supportsTransactionIsolationLevel(1));
			System.out.println("whetehr support transaction: " + dbMetaData.supportsTransactions());
			System.out.println("whether support SQL UNION:" + dbMetaData.supportsUnion());
			System.out.println("whether support SQL UNION ALL:" + dbMetaData.supportsUnionAll());
			System.out.println("use local file for each table? " + dbMetaData.usesLocalFilePerTable());
			System.out.println("whether store table in local file:" + dbMetaData.usesLocalFiles());
			System.out.println("major version of database: " + dbMetaData.getDatabaseMajorVersion());
			System.out.println("minor version of database: " + dbMetaData.getDatabaseMinorVersion());
			System.out.println("JDBC majoir version: " + dbMetaData.getJDBCMajorVersion());
			System.out.println("JDBC minor version: " + dbMetaData.getJDBCMinorVersion());
			System.out.println("JDBC driver name: " + dbMetaData.getDriverName());
			System.out.println("JDBC driver version:" + dbMetaData.getDriverVersion());
			System.out.println("extral characters: " + dbMetaData.getExtraNameCharacters());
			System.out.println("string to invoke sql: " + dbMetaData.getIdentifierQuoteString());
			System.out.println("getMaxCatalogNameLength:" + dbMetaData.getMaxCatalogNameLength());
			System.out.println("getMaxColumnNameLength:" + dbMetaData.getMaxColumnNameLength());
			System.out.println("getMaxColumnsInGroupBy:" + dbMetaData.getMaxColumnsInGroupBy());
			System.out.println("getMaxColumnsInSelect:" + dbMetaData.getMaxColumnsInSelect());
			System.out.println("getMaxColumnsInTable:" + dbMetaData.getMaxColumnsInTable());
			System.out.println("getMaxConnections:" + dbMetaData.getMaxConnections());
			System.out.println("getMaxCursorNameLength:" + dbMetaData.getMaxCursorNameLength());
			System.out.println("getMaxStatements: " + dbMetaData.getMaxStatements());
		} catch (Exception e) {
			System.err.println("SQLException :" + e.getMessage());
			e.printStackTrace();
		}
	}
}
