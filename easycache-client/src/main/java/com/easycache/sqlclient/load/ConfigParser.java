//jiang yong
//easycache configurations
package com.easycache.sqlclient.load;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.easycache.sqlclient.jdbc.IMDGSqlFilter;
import com.hazelcast.easycache.serialization.HazelcastObjectSerializer;
import com.hazelcast.easycache.utility.IMDGString;

public class ConfigParser {

	private static String dbType = null;
	private static boolean dbTypeIsParsed = false;
	private static String configFilePath = "EasyCacheConfig.properties";

	// singleton
	private static ConfigParser configParser = null;

	private ConfigParser() {
	}

	public static ConfigParser getInstance() {
		if (configParser == null) {
			configParser = new ConfigParser();
		}
		return configParser;
	}

	public static String getConfigFilePath() {
		return configFilePath;
	}

	public static void loaderConfigParse() {
		try {
			Properties prop = new Properties();
			// File file1 = new File(this.getClass().getResource("/").getPath()+
			// "../loadData.properties");
			// System.out.println(file1.getAbsolutePath());
			InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
			if (in != null) {
				prop.load(in);
				String loadConcurrently = prop.getProperty("loadConcurrently").trim();
				int pageSize = Integer.parseInt(prop.getProperty("pageSize"));
				int maxThreadPoolSize = Integer.parseInt(prop.getProperty("maxThreadPoolSize"));
				if (loadConcurrently.equals("true")) {
					Loader.setThreadParam(true, pageSize, maxThreadPoolSize);
				} else if (loadConcurrently.equals("false")) {
					Loader.setThreadParam(false, pageSize, maxThreadPoolSize);
				} else {
					throw new IOException("loadConcurrently should be true or false, " + loadConcurrently
							+ " is not reasonable!");
				}

			} else {
				throw new IOException("warning :can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void executorConfigParse() {
		try {
			Properties prop = new Properties();
			// File file1 = new File(Loader.class.getResource("/").getPath()+
			// "../config.properties");
			// System.out.println(file1.getAbsolutePath());
			InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
			if (in != null) {
				prop.load(in);
				String selectFilter = prop.getProperty("selectFilter");
				if (selectFilter != null) {
					selectFilter = selectFilter.toLowerCase();
				}
				String insertFilter = prop.getProperty("insertFilter");
				if (insertFilter != null) {
					insertFilter = insertFilter.toLowerCase();
				}
				String deleteFilter = prop.getProperty("deleteFilter");
				if (deleteFilter != null) {
					deleteFilter = deleteFilter.toLowerCase();
				}
				String updateFilter = prop.getProperty("updateFilter");
				if (updateFilter != null) {
					updateFilter = updateFilter.toLowerCase();
				}
				IMDGSqlFilter.setSqlFilter(stringSplit(selectFilter), stringSplit(insertFilter),
						stringSplit(deleteFilter), stringSplit(updateFilter));
			} else {
				throw new IOException("warning :can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String configParseGetDbType() {
		if (!dbTypeIsParsed) {
			try {
				Properties prop = new Properties();
				InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
				if (in != null) {
					dbTypeIsParsed = true;
					prop.load(in);
					dbType = prop.getProperty("dbType");
					if (dbType != null) {
						dbType = dbType.trim().toLowerCase();
					} else {
						throw new IOException("Warning: dbType is null.");
					}
					return dbType;
				} else {
					throw new IOException("warning :can't find EasyCacheConfig.properties");
				}
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		} else {
			return dbType;
		}
	}

	public String configParseGetSchema() {
		try {
			Properties prop = new Properties();
			InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
			if (in != null) {
				prop.load(in);
				String schema = prop.getProperty("schema");
				if (schema == null) {
					throw new IOException("Warning: cacheSwitch is null.");
				}
				return schema.trim();
			} else {
				throw new IOException("warning :can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void pstOptimizationConfigParse() {
		try {
			Properties prop = new Properties();
			InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
			if (in != null) {
				prop.load(in);
				String pstOptimization = prop.getProperty("pstOptimization");
				if (pstOptimization != null) {
					pstOptimization = pstOptimization.trim().toLowerCase();
				} else {
					throw new IOException("Warning: pstOptimization is null.");
				}
				if (pstOptimization.equals("true")) {
					IMDGSqlFilter.setPstOptimization(true);
				} else if (pstOptimization.equals("false")) {
					IMDGSqlFilter.setPstOptimization(false);
				} else {
					throw new IOException("pstOptimization should be true or false, " + pstOptimization
							+ " is not reasonable!");
				}
			} else {
				throw new IOException("warning :can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void localOptimizationConfigParse() {
		try {
			Properties prop = new Properties();
			InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
			if (in != null) {
				prop.load(in);
				String localOptimization = prop.getProperty("localOptimization");
				if (localOptimization != null) {
					localOptimization = localOptimization.trim().toLowerCase();
				} else {
					throw new IOException("Warning: localOptimization is null.");
				}
				if (localOptimization.equals("true")) {
					HazelcastObjectSerializer.setLocalOptimization(true);
				} else if (localOptimization.equals("false")) {
					HazelcastObjectSerializer.setLocalOptimization(false);
				} else {
					throw new IOException("pstOptimization should be true or false, " + localOptimization
							+ " is not reasonable!");
				}
			} else {
				throw new IOException("warning :can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void queryResultCacheConfigParse() {
		try {
			Properties prop = new Properties();
			// File file1 = new File(Loader.class.getResource("/").getPath()+
			// "../config.properties");
			// System.out.println(file1.getAbsolutePath());
			InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
			if (in != null) {
				prop.load(in);
				String cacheSwitch = prop.getProperty("cacheSwitch");
				if (cacheSwitch != null) {
					cacheSwitch = cacheSwitch.trim().toLowerCase();
				} else {
					throw new IOException("Warning: cacheSwitch is null.");
				}
				if (cacheSwitch.equals("true")) {
					IMDGSqlFilter.setCacheSwitch(true);
				} else if (cacheSwitch.equals("false")) {
					IMDGSqlFilter.setCacheSwitch(false);
				} else {
					throw new IOException("cacheSwitch should be true or false, " + cacheSwitch
							+ " is not reasonable!");
				}
				String sqlToCache = prop.getProperty(IMDGString.SQL_TO_CACHE);
				if (sqlToCache != null) {
					sqlToCache = sqlToCache.toLowerCase();
				}
				IMDGSqlFilter.setSqlToCache(stringSplit(sqlToCache));
			} else {
				throw new IOException("warning :can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void loadPartiallyConfigParse() {
		try {
			Properties prop = new Properties();
			InputStream in = ConfigParser.class.getResourceAsStream(configFilePath);
			if (in != null) {
				prop.load(in);
				String loadPartially = prop.getProperty("loadPartially");
				if (loadPartially != null) {
					loadPartially = loadPartially.trim().toLowerCase();
				} else {
					throw new IOException("Warning: loadPartially is null.");
				}
				if (loadPartially.equals("true")) {
					Loader.setLoadPartially(true);
				} else if (loadPartially.equals("false")) {
					Loader.setLoadPartially(false);
				} else {
					throw new IOException("loadPartially should be true or false, " + loadPartially
							+ " is not reasonable!");
				}
				String loadTables = prop.getProperty("loadTables");
				if (loadTables != null) {
					loadTables = loadTables.toLowerCase();
				}

				Loader.setLoadTables(stringSplit(loadTables));
			} else {
				throw new IOException("warning :can't find EasyCacheConfig.properties");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String[] stringSplit(String filter) {
		if (filter == null || filter.equals("")) {
			return null;
		} else {
			return filter.trim().split("\\$#@");
		}
	}

}
