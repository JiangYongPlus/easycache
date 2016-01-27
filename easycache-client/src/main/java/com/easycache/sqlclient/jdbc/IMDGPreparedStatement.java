package com.easycache.sqlclient.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.easycache.utility.IMDGString;
import com.easycache.sqlclient.executor.HazelcastExecutor;
import com.easycache.sqlclient.log.IMDGPreparedStatementLog;
import com.easycache.sqlclient.type.DataType;
import com.easycache.sqlclient.type.SqlKind;
import com.easycache.sqlclient.utility.ThreadPool;

public class IMDGPreparedStatement implements java.sql.PreparedStatement{
	private static ConcurrentHashMap<String, ArrayList<String>> arguMap = new ConcurrentHashMap<String, ArrayList<String>>(100);
	private static ConcurrentHashMap<String, Integer> updateSetArguNumMap = new ConcurrentHashMap<String, Integer>(100);
	
	BatchParams batchParams =  null;
	private static int batchThreadNum = 1;
	private int blockSize;
	private HashMap<String, ArrayList<String>> arguValueListMap = null;
	protected String[] originalSql = null;
	protected ResultSetMetaData rsmd = null;
	// parse sql sentence
	protected String sqlsentence = null;
	protected int argucnt = 0;
	protected ArrayList<String> arguNameList = null;
	protected ArrayList<String> arguValueList = new ArrayList<String>();
	// for update sql
	protected Integer setargunum = null;
	protected ArrayList<String> updatesetArguList = null;
	protected ArrayList<String> updatewhereArguList = null;
	protected SqlKind sqlkind;
	
	protected HazelcastExecutor hzExecutor = new HazelcastExecutor();
	protected IMDGSqlFilter imdgSqlFilter = new IMDGSqlFilter();
	protected IMDGResultSet hzrs = null;
	protected int updateCount = -1;
	protected boolean closed = false;
	protected Connection connection = null;
	
	protected int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
	protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
	protected boolean isReadOnly = false;
	
	private static HazelcastInstance hazelcast = null;
	
	
	public IMDGPreparedStatement(String sql, Connection c, SqlKind kind) throws SQLException {
		IMDGPreparedStatementLog.showSql(sql);
		
		//jiang yong 2014-11-2 for query result cache
		if((hazelcast = Hazelcast.getHazelcastInstanceByName("IMDG")) == null) {
			Config config = new Config();
			config.setInstanceName("IMDG");
			hazelcast = Hazelcast.newHazelcastInstance(config);
		}
		//added done
		
		sqlsentence = sql;
		connection = c;
		sqlkind = kind;
		
		if(sqlkind == SqlKind.SELECT || sqlkind == SqlKind.INSERT) {
			hzrs = new IMDGResultSet(this);
		}
		
		arguNameList = checkSQLParsed(sql);
		if (arguNameList != null) {
			argucnt = arguNameList.size();
			arguValueList.clear();
			for (int i = 0; i < argucnt; ++i) {
				arguValueList.add(null);
			}
		}
		sql = sql.trim();
		if (sql.endsWith("?")) {
			sql = sql + " ";
		}
		originalSql = sql.split("\\?");
	}
	
	private void addArgument(String key, ArrayList<String> names, int num, Boolean updateflag) {
		arguMap.put(key, names);
		// update condition: count the arguments in the set clause
		if(updateflag) {
			updateSetArguNumMap.put(key, new Integer(num));
		}
	}
	
	//jiang yong 2014-12-12
	public void addBatch() throws SQLException {
		if (batchParams == null) {
			batchParams = new BatchParams(sqlkind, sqlsentence, arguNameList);
		}
		batchParams.addArguValueList(arguValueList);
	}
	
	public void addBatch(String sql) throws SQLException {
		throw new SQLException("addBatch(String sql) Not Implemented.");
	}
	
	public void cancel() throws SQLException {
	}

	protected void checkClosed() throws SQLException {
		if (this.closed) {
			throw new SQLException("SQL_STATE_CONNECTION_NOT_OPEN");
		}
	}
	
	private void CheckParamIndex(int paramIndex) throws SQLException {
		if(paramIndex < 1 || paramIndex > argucnt) {
			throw new SQLException("ilegal parameter index");
		}
	}
	
	private ArrayList<String> checkSQLParsed(String sql) throws SQLException{
		ArrayList<String> tmp = arguMap.get(sql);
		if (tmp == null) {
			switch (sqlkind) {
			case SELECT:
				addArgument(sql, parseSelectSQL(sql), -1, false);
				break;
			case INSERT:
				addArgument(sql, parseInsertSQL(sql), -1, false);
				break;
			case DELETE:
				addArgument(sql, parseDeleteSQL(sql), -1, false);
				break;
			case UPDATE:
				ArrayList<String> tmpList = new ArrayList<String>();
				int setargunum = parseUpdateSQL(sql, tmpList);
				addArgument(sql, tmpList, setargunum, true);
				break;
			default:
				throw new SQLException("supportedCheck(): Unknow SQL Type");
			}
		}
		return ((tmp == null) ? arguMap.get(sql) : tmp);
	}
	
	protected void clear() throws SQLException {
		if (hzrs != null) {
			hzrs.clear();
		}
		updateCount = -1;
	}
	
	
	public void clearBatch() throws SQLException {
		if(batchParams != null){
			batchParams.clear();
		}
		batchParams = null;
	}
	
	public void clearParameters() throws SQLException {
		arguValueList.clear();
		for (int i = 0; i < argucnt; i++) {
			arguValueList.add(null);
		}
	}
	
	public void clearWarnings() throws SQLException {
	}
	
	public void close() throws SQLException {
		closed = true;
		if(hzrs != null) {
			hzrs.close();
		}
		originalSql = null;
		rsmd = null;
		// parse sql sentence
		sqlsentence = null;
		arguNameList = null;
		arguValueList = null;
		// for update sql
		updatesetArguList = null;
		updatewhereArguList = null;
		// temporary code to uncouple IMDGPreparedStatement from IMDGStatement
		hzExecutor = null;
		hzrs = null;
		connection = null;
		updateCount = -1;
	}
	
	@SuppressWarnings("unused")
	private void debugInfo() {
		System.err.println("*****************************************************************************");
		System.err.println("Cached SQL:" + sqlsentence);
		for (int i = 1; i <= argucnt; i++) {
			System.err.println("[" + arguNameList.get(i-1) + ":" + arguValueList.get(i-1) + "]");
		}
		System.err.println("*****************************************************************************");
	}

	public boolean execute() throws SQLException {
		IMDGPreparedStatementLog.funcLog("execute()");
		if (sqlkind == SqlKind.SELECT) {
			executeQuery();
			return true;
		} else if (sqlkind == SqlKind.INSERT || sqlkind == SqlKind.UPDATE || sqlkind == SqlKind.DELETE) {
			executeUpdate();
			return false;
		} else {
			throw new SQLException("Unknow SQL Type");
		}
	}
	
	
	public boolean execute(String sql) throws SQLException {
		throw new SQLException("execute(String sql) Not Implemented.");
	}
	
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLException("execute(String sql, int autoGeneratedKeys) Not Implemented.");
	}
	
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		// too complex
		//IMDGPreparedStatementLog.showInfo("execute(String sql, int[] columnIndexes)");
		throw new SQLException("execute(String sql, int[] columnIndexes) Not Implemented.");
	}

	
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLException("execute(String sql, String[] columnNames) Not Implemented.");
	}

	
	public int[] executeBatch() throws SQLException {
		IMDGPreparedStatementLog.funcLog("executeBatch()");
		checkClosed();
		sqlkind = batchParams.getSqlKind();
		sqlsentence = batchParams.getSqlSentence();
		arguNameList = batchParams.getArguNameList();
		arguValueListMap = batchParams.getArguValueListMap();
		ExecutorService executor = ThreadPool.getInstance().getExecutor();
		blockSize = arguValueListMap.size() / batchThreadNum + 1;
		ArrayList<Future<?>> futureList = new ArrayList<Future<?>>();
		for (int i = 0; i < batchThreadNum; i++) {
			Runnable executeBatchSubTask = new ExecuteBatchSubTask(i);
			futureList.add(executor.submit(executeBatchSubTask));
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
		return null;
	}

	//jiang yong 2014-11-2 for query result cache
	public IMDGResult resultSetToResult(IMDGResultSet imdgResultSet){
		IMDGResult imdgResult = new IMDGResult();
//		imdgResult.setImdgResultInfo(true);
		imdgResult.resultCols = imdgResultSet.resultCols;
		imdgResult.resultRows = imdgResultSet.resultRows;
		imdgResult.resultCurrent = imdgResultSet.resultCurrent;
		imdgResult.lastRead = imdgResultSet.lastRead;
		for(Map.Entry<String, Integer> entry: imdgResultSet.colsNameMap.entrySet()){
			imdgResult.colsNameMap.put(entry.getKey(), entry.getValue());
		}
		for(DataType.Type colsType : imdgResultSet.colsTypeList){
			imdgResult.colsTypeList.add(colsType);
		}
		for(String colsTab : imdgResultSet.colsTabList){
			imdgResult.colsTabList.add(colsTab);
		}
		for(String colsDB: imdgResultSet.colsDBList){
			imdgResult.colsDBList.add(colsDB);
		}
		for(int i = 0; i < imdgResultSet.colsValuesList.size(); i++){
			imdgResult.colsValuesList.add(new ArrayList<Object>());
			ArrayList<Object> rowlist = imdgResultSet.colsValuesList.get(i);
			for(int j = 0; j < rowlist.size(); j++){
				imdgResult.colsValuesList.get(i).add(rowlist.get(j));
			}
		}
		return imdgResult;
	}
	
	public IMDGResultSet resultToResultSet(IMDGResult imdgResult){
		try{
			IMDGResultSet imdgResultSet = new IMDGResultSet(this);
			imdgResultSet.resultCols = imdgResult.resultCols;
			imdgResultSet.resultRows = imdgResult.resultRows;
			imdgResultSet.resultCurrent = imdgResult.resultCurrent;
			imdgResultSet.lastRead = imdgResult.lastRead;
			for(Map.Entry<String, Integer> entry: imdgResult.colsNameMap.entrySet()){
				imdgResultSet.colsNameMap.put(entry.getKey(), entry.getValue());
			}
			for(DataType.Type colsType : imdgResult.colsTypeList){
				imdgResultSet.colsTypeList.add(colsType);
			}
			for(String colsTab : imdgResult.colsTabList){
				imdgResultSet.colsTabList.add(colsTab);
			}
			for(String colsDB: imdgResult.colsDBList){
				imdgResultSet.colsDBList.add(colsDB);
			}
			for(int i = 0; i < imdgResult.colsValuesList.size(); i++){
				imdgResultSet.colsValuesList.add(new ArrayList<Object>());
				ArrayList<Object> rowlist = imdgResult.colsValuesList.get(i);
				for(int j = 0; j < rowlist.size(); j++){
					imdgResultSet.colsValuesList.get(i).add(rowlist.get(j));
				}
			}
			return imdgResultSet;
		}catch(SQLException e){
			e.printStackTrace();
			return null;
		}
	}
	
	public IMDGResult dbResultSetToIMDGResult(ResultSet dbResultSet){
		try{
			IMDGResult imdgResult = new IMDGResult();
//			imdgResult.setImdgResultInfo(true);
			ResultSetMetaData rsData = dbResultSet.getMetaData();
			ArrayList<String> namesList = new ArrayList<String>();
			ArrayList<String> typeNameList = new ArrayList<String>();
			ArrayList<String> colsTabList = new ArrayList<String>();
			ArrayList<String> colsDBList = new ArrayList<String>();
			ArrayList<List<Object> > colsValueList = new ArrayList<List<Object> >();

			int columncount = rsData.getColumnCount();
			for (int i = 1; i <= columncount; i++) {
				String columnName = rsData.getColumnName(i).toUpperCase();
				namesList.add(columnName);

				String className = rsData.getColumnClassName(i).toUpperCase();
				typeNameList.add(className);

				colsTabList.add(rsData.getTableName(i).toUpperCase());
				colsDBList.add(rsData.getCatalogName(i).toUpperCase());
			}
			imdgResult.addMetaData(namesList, typeNameList, colsTabList, colsDBList);
			while (dbResultSet.next()) {
				ArrayList<Object>  rowlist = new ArrayList<Object>();
				columncount = rsData.getColumnCount();
				for (int i = 1; i <= columncount; i++) {
					rowlist.add(dbResultSet.getObject(i));
				}
				colsValueList.add((List<Object>)rowlist);
			}
			imdgResult.addRowValueData(colsValueList);
			imdgResult.checkResult();
			dbResultSet.close();
			return imdgResult;
		}catch(SQLException e){
			e.printStackTrace();
			return null;
		}	
	}
	
	public ResultSet executeQuery() throws SQLException{
		IMDGPreparedStatementLog.funcLog("executeQuery()");
		if(imdgSqlFilter.getCacheSwitch() == false){
			return executeQueryNoQueryCache();
		} else {
			if(imdgSqlFilter.supportedQueryResultCache(sqlsentence)){
				checkClosed();
				clear();
				IMap<String, IMDGResult> map = hazelcast.getMap(IMDGString.QUERY_RESULT_CACHE);
				String sql = SQLConcat();
				if(map.containsKey(sql)){
//					System.out.println("cached: " +  sql);
					IMDGResult rst = map.get(sql);
					return rst != null? resultToResultSet(rst) : null;
				} else {
					int tmp = hzExecutor.executeSelect(sqlsentence, arguNameList, arguValueList, hzrs);
					hzrs.checkResult();
					if (tmp == 0) {
						if (hzrs != null) {
							map.put(sql, resultSetToResult(hzrs));
							return resultToResultSet(map.get(sql));
						} else {
							map.put(sql, null);
							return null;
						}
					} else {
						throw new SQLException("executeQuery(): wrong result returned");
					}
				}//map.containsKey(sql) end
			} else {
				return executeQueryNoQueryCache();
			}//supportedQueryResultCache(sqlsentence) end
		}//getCacheSwitch().equals("false") end
	}
	
	public ResultSet executeQueryNoQueryCache() throws SQLException {
		checkClosed();
		clear();
		int tmp = hzExecutor.executeSelect(sqlsentence, arguNameList, arguValueList, hzrs);
		hzrs.checkResult();
		if (tmp == 0) {
			return hzrs;
		} else {
			throw new SQLException("executeQuery(): wrong result returned");
		}
	}
	
	
	// temporary code to decouple IMDGPreparedStatement from IMDGStatement
	public ResultSet executeQuery(String sql) throws SQLException {
		throw new SQLException("executeQuery(String sql) Not Implemented.");
	}

	
	public int executeUpdate() throws SQLException {
		IMDGPreparedStatementLog.funcLog("executeUpdate()");
		checkClosed();
		clear(); // must!!
		switch (sqlkind) {
		case INSERT:
			updateCount = hzExecutor.executeInsert(sqlsentence, arguNameList, arguValueList, hzrs);
			hzrs.checkResult();
			return updateCount;
		case DELETE:
			updateCount = hzExecutor.executeDelete(sqlsentence, arguNameList, arguValueList);
			return updateCount;
		case UPDATE:
			if (setargunum == null) {
				setargunum = IMDGPreparedStatement.updateSetArguNumMap.get(sqlsentence);
			}
			if (updatesetArguList == null) {
				updatesetArguList = new ArrayList<String>(arguNameList.subList(0, setargunum));
				updatewhereArguList = new ArrayList<String>(arguNameList.subList(setargunum, arguNameList.size()));
			}
			ArrayList<String> updatesetValueList = new ArrayList<String>(arguValueList.subList(0, setargunum));
			ArrayList<String> updatewhereValueList = new ArrayList<String>(arguValueList.subList(setargunum, arguValueList.size()));
			updateCount = hzExecutor.executeUpdate(sqlsentence, updatesetArguList, updatesetValueList,
					updatewhereArguList, updatewhereValueList);
			return updateCount;
		default:
			throw new SQLException("Unknow SQL Type");
		}
	}

	public int executeUpdate(String sql) throws SQLException {
		throw new SQLException(" executeUpdate(String sql) Not Implemented.");
	}

	
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLException(" executeUpdate(String sql, int autoGeneratedKeys) Not Implemented.");
	}

	
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		throw new SQLException("executeUpdate(String sql, int[] columnIndexes) Not Implemented.");
	}

	
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLException("executeUpdate(String sql, String[] columnNames) Not Implemented.");
	}

	
	public Connection getConnection() throws SQLException {
		IMDGPreparedStatementLog.funcLog("getConnection()");
		return this.connection;
	}

	
	public int getFetchDirection() throws SQLException {
		return java.sql.ResultSet.FETCH_FORWARD;
	}

	
	public int getFetchSize() throws SQLException {
		IMDGPreparedStatementLog.funcLog("getFetchSize()");
		return 0;
	}

	
	public ResultSet getGeneratedKeys() throws SQLException {
		IMDGPreparedStatementLog.funcLog("getGeneratedKeys()");
		if(!hzrs.isClosed()){
			return hzrs;
		} else {
			throw new SQLException("No Available ResultSet");
		}
	}

	public int getMaxFieldSize() throws SQLException {
		// For Version 2
//		return fetchSize;
		IMDGPreparedStatementLog.funcLog("getMaxFieldSize()");
		return 0;
	}

	
	public int getMaxRows() throws SQLException {
		// get the max rows of the resultSet can contain, if over the limit, revoke the extra rows
		// 0 means no limit
		IMDGPreparedStatementLog.funcLog("getMaxRows()");
		return 0;
	}

	
	public ResultSetMetaData getMetaData() throws SQLException {
		// HashMap<Integer, String> colIndexMap need to be written by parsing sql statement
		// no supported yet
		// TODO
//		if (rsmd == null) {
//			rsmd = dbst.getMetaData();
//		}
//		return rsmd;
		IMDGPreparedStatementLog.funcLog("getMetaData()");
		return null;
	}
	
	public boolean getMoreResults() throws SQLException {
		IMDGPreparedStatementLog.funcLog("getMoreResults()");
		return false;
	}
	
	
	public boolean getMoreResults(int current) throws SQLException {
		// TODO Auto-generated method stub
		IMDGPreparedStatementLog.funcLog("getMoreResults(int current)");
		return false;
	}

	
	public ParameterMetaData getParameterMetaData() throws SQLException {
		// no supported yet
		IMDGPreparedStatementLog.funcLog("getParameterMetaData()");
		return new IMDGParametermetaData();
	}

	
	public int getQueryTimeout() throws SQLException {
		// get the statement Objects executing time, if over the limit, throw sqlException.
		// 0 means no limit
		IMDGPreparedStatementLog.funcLog("getQueryTimeout()");
		return 0;
	}

	
	public ResultSet getResultSet() throws SQLException {
		IMDGPreparedStatementLog.funcLog("getResultSet()");
		if(!hzrs.isClosed()) {
			return hzrs;
		} else {
			throw new SQLException("No Available ResultSet");
		}
	}
	
	public ResultSet getResultSet_bk() throws SQLException {
		if(!hzrs.isClosed()) {
			return hzrs;
		} else {
			throw new SQLException("No Available ResultSet");
		}
	}

	
	public int getResultSetConcurrency() throws SQLException {
		IMDGPreparedStatementLog.funcLog("getResultSetConcurrency()");
		// TODO Auto-generated method stub
		return 0;
	}

	
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		IMDGPreparedStatementLog.funcLog("getResultSetHoldability()");
		return 0;
	}

	
	public int getResultSetType() throws SQLException {
		// TODO Auto-generated method stub
		IMDGPreparedStatementLog.funcLog("getResultSetType()");
		return 0;
	}

	
	public int getUpdateCount() throws SQLException {
		IMDGPreparedStatementLog.funcLog("getUpdateCount()");
		return updateCount;
	}

	
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public boolean isClosed() throws SQLException {
		IMDGPreparedStatementLog.funcLog("isClosed()");
		return closed;
	}

	
	public boolean isPoolable() throws SQLException {
		IMDGPreparedStatementLog.funcLog("isPoolable()");
		// TODO Auto-generated method stub
		return false;
	}

	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		IMDGPreparedStatementLog.funcLog("isWrapperFor(Class<?> iface)");
		return false;
	}

	
	private ArrayList<String> parseDeleteSQL(String sql) {
		IMDGPreparedStatementLog.funcLog("parseDeleteSQL(String sql)");
		return parseSelectSQL(sql);
	}

	
	private ArrayList<String> parseInsertSQL(String sql) {
		String str = sql.trim().split("[()]")[1];
		String[] values = str.trim().split("[, ]+");
		ArrayList<String> results = new ArrayList<String>();
		Collections.addAll(results, values);
		return results;
	}

	
	private ArrayList<String> parseSelectSQL(String sql) {
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<String> results = new ArrayList<String>();
		String[] strs = sql.trim().split("'");
		int length = strs.length;
		for (int j = 0; j < length; j++) {
			if(j%2 == 0) {
				String[] tmps = strs[j].trim().split("[ ,()]+");
				for (String str : tmps) {
					//toLowerCase?
//					values.add(str.toLowerCase());
					values.add(str);
				}
			} else {
				values.add("'" + strs[j] + "'");
			}
		}
		length = values.size();
		for (int t = 0; t < length; t++) {
			if(values.get(t).equals("?")) {
				String name = values.get(t-2);
				int index = name.indexOf('.');
				if(index != -1) {
					results.add(name.substring(++index));
				} else {
					results.add(name);
				}
			}
		}
		return results;
	}

	
	private int parseUpdateSQL(String sql, ArrayList<String> outList) {
		ArrayList<String> tmp = parseSelectSQL(sql);
		int index = sql.toLowerCase().indexOf(" where ");
		int num = 0;
		int length = sql.length();
		for(;index < length; ++index) {
			if(sql.charAt(index) == '?') {
				++num;
			}
		}
		num = tmp.size() - num;
		outList.clear();
		outList.addAll(tmp);
		return num;
	}

	
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLException("No Implemented.");
		
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		// most 1024 bytes
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		byte[] bytes = new byte[1025];
		try {
			x.read(bytes, 0, 1024);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(bytes));
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		// different with MySQL
		// read from stream into a string
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		byte[] bytes = new byte[length + 1];
		try {
			x.read(bytes, 0, length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(bytes));
	}

	
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		// same problem with the above
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		byte[] bytes = new byte[(int) (length+1L)];
		try {
			x.read(bytes, 0, (int) length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(bytes));
	}

	
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setValue(parameterIndex, null);
		} else {
			setValue(parameterIndex, String.valueOf(x));
		}
	}

	
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		setAsciiStream(parameterIndex, x);
	}

	
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		// same problem with the above
		setAsciiStream(parameterIndex, x, length);
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		setAsciiStream(parameterIndex, x, length);
	}

	
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
		} else {
			setString(parameterIndex, new String(x.getBytes(0, (int)x.length())));
		}
	}

	
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		setAsciiStream(parameterIndex, inputStream);
	}

	
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		setAsciiStream(parameterIndex, inputStream, length);
	}

	
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if(x) {
			setValue(parameterIndex, "true");
		} else {
			setValue(parameterIndex, "false");
		}
	}

	
	public void setByte(int parameterIndex, byte x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
		} else {
			setString(parameterIndex, new String(x));
		}
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		// most 1024 bytes
		CheckParamIndex(parameterIndex);
		if (reader == null) {
			setValue(parameterIndex, null);
			return;
		}
		char[] buffer = new char[1025];
		try {
			reader.read(buffer, 0, 1024);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(buffer));
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		// same problem with the above
		CheckParamIndex(parameterIndex);
		if (reader == null) {
			setString(parameterIndex, null);
			return;
		}
		char[] buffer = new char[length+1];
		try {
			reader.read(buffer, 0, length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setString(parameterIndex, new String(buffer));
	}

	
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		setCharacterStream(parameterIndex, reader, (int)length);
	}

	public void setClob(int parameterIndex, Clob x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
		} else {
			setString(parameterIndex, x.getSubString(1L, (int)x.length()));
		}
	}

	
	
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader);
	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		setCharacterStream(parameterIndex, reader, (int)length);
	}

	public void setCursorName(String name) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setDate(int parameterIndex, Date x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		} 
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		setString(parameterIndex, dateFormatter.format(x));
	}

	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		setDate(parameterIndex, x);
	}

	public void setDouble(int parameterIndex, double x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setFetchDirection(int direction) throws SQLException {
		switch (direction) {
		case java.sql.ResultSet.FETCH_FORWARD:
		case java.sql.ResultSet.FETCH_REVERSE:
		case java.sql.ResultSet.FETCH_UNKNOWN:
			break;

		default:
			throw new SQLException("SQL_STATE_ILLEGAL_ARGUMENT");
		}
		
	}

	public void setFetchSize(int rows) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
//		parameterStrings[parameterIndex-1] = String.valueOf(x);
	}

	public void setInt(int parameterIndex, int x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	public void setLong(int parameterIndex, long x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	public void setMaxFieldSize(int max) throws SQLException {
		// For Version 2
//		if (max < 0) {
//			throw new SQLException("ilegal argument");
//		}
//		fetchSize = max;
	}

	public void setMaxRows(int max) throws SQLException {
//		if (max <= 0) {
//			throw new SQLException("ilegal argument");
//		}
//		maxRows = max;
	}

	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		setCharacterStream(parameterIndex, value);
	}

	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		setCharacterStream(parameterIndex, value, (int)length);		
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		// no supported yet
		throw new SQLException("No Implemented.");
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader);
	}

	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setNString(int parameterIndex, String value)
			throws SQLException {
		setString(parameterIndex, value);
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, null);
	}

	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		setNull(parameterIndex, sqlType);
	}

	//TODO: datatype 集中管理
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.OTHER);
		} else {
			if (x instanceof Byte) {
				setInt(parameterIndex, ((Byte) x).intValue());
			} else if (x instanceof String) {
				setString(parameterIndex, (String) x);
			} else if (x instanceof BigDecimal) {
				setBigDecimal(parameterIndex, (BigDecimal) x);
			} else if (x instanceof Short) {
				setShort(parameterIndex, ((Short) x).shortValue());
			} else if (x instanceof Integer) {
				setInt(parameterIndex, ((Integer) x).intValue());
			} else if (x instanceof Long) {
				setLong(parameterIndex, ((Long) x).longValue());
			} else if (x instanceof Float) {
				setFloat(parameterIndex, ((Float) x).floatValue());
			} else if (x instanceof Double) {
				setDouble(parameterIndex, ((Double) x).doubleValue());
			} else if (x instanceof byte[]) {
				setBytes(parameterIndex, (byte[]) x);
			} else if (x instanceof java.sql.Date) {
				setDate(parameterIndex, (java.sql.Date) x);
			} else if (x instanceof Time) {
				setTime(parameterIndex, (Time) x);
			} else if (x instanceof Timestamp) {
				setTimestamp(parameterIndex, (Timestamp) x);
			} else if (x instanceof Boolean) {
				setBoolean(parameterIndex, ((Boolean) x)
						.booleanValue());
			} else if (x instanceof InputStream) {
				setBinaryStream(parameterIndex, (InputStream) x, -1);
			} else if (x instanceof java.sql.Blob) {
				setBlob(parameterIndex, (java.sql.Blob) x);
			} else if (x instanceof java.sql.Clob) {
				setClob(parameterIndex, (java.sql.Clob) x);
			} else if (x instanceof java.util.Date) {
				setTimestamp(parameterIndex, new Timestamp(((java.util.Date) x).getTime()));
			} else if (x instanceof BigInteger) {
				setString(parameterIndex, x.toString());
			} else {
				setSerializableObject(parameterIndex, x);
			}
		}
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		setObject(parameterIndex, x);
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setPoolable(boolean poolable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	void setReadOnly(boolean isReadOnly) {
		this.isReadOnly = isReadOnly;
	}

	public void setRef(int parameterIndex, Ref x) throws SQLException {
			throw new SQLException("No Implemented.");
	}

	void setResultSetConcurrency(int resultSetConcurrency) {
		this.resultSetConcurrency = resultSetConcurrency;
	}

	void setResultSetType(int resultSetType) {
		this.resultSetType = resultSetType;
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		// no supported yet
		throw new SQLException("No Implemented.");
	}

	private final void setSerializableObject(int parameterIndex,
			Object parameterObj) throws SQLException {
		try {
			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
			objectOut.writeObject(parameterObj);
			objectOut.flush();
			objectOut.close();
			bytesOut.flush();
			bytesOut.close();

			byte[] buf = bytesOut.toByteArray();
			ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);
			setBinaryStream(parameterIndex, bytesIn, buf.length);
		} catch (Exception ex) {
			throw new SQLException(ex.getMessage());
		}
	}

	public void setShort(int parameterIndex, short x) throws SQLException {
		CheckParamIndex(parameterIndex);
		setValue(parameterIndex, String.valueOf(x));
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setString(int parameterIndex, String x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setValue(parameterIndex, null);
			return;
		} else {
			//toLowerCase?
			//arguValueList.set(parameterIndex - 1, "'" + x.toLowerCase() + "'");
			arguValueList.set(parameterIndex - 1, "'" + x + "'");
		}
	}

	public void setTime(int parameterIndex, Time x) throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		setString(parameterIndex, x.toString());
	}

	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		setTime(parameterIndex, x);
	}

	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		CheckParamIndex(parameterIndex);
		if (x == null) {
			setString(parameterIndex, null);
			return;
		}
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String time1 = formatter.format(x);
		setString(parameterIndex, time1);
	}

	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		setTimestamp(parameterIndex, x);
	}

	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		setAsciiStream(parameterIndex, x, length);
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
		if (x != null) {
			setString(parameterIndex, x.toString());
		} else {
			setNull(parameterIndex, Types.CHAR);
		}
	}
	
	private void setValue(int parameterIndex, String value) {
			if(value == null) {
				arguValueList.set(parameterIndex - 1, "null");
			} else {
				arguValueList.set(parameterIndex - 1, value);
			}
	}

	private String SQLConcat() {
		StringBuffer buffer = new StringBuffer(originalSql[0]);
		if(arguValueList.size() > 0) {
			int len = arguValueList.size();
			for(int i = 0; i < len; ++i) {
				buffer.append(arguValueList.get(i));
				buffer.append(originalSql[i+1]);
			}
		}
		//toLowerCase?
		//return buffer.toString().trim().toLowerCase();
//		String sql = buffer.toString().trim();
//		System.out.println("sqlConcat:" + sql);
		return buffer.toString().trim();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	//TODO: hzrs here is for what?
	class ExecuteBatchSubTask implements Runnable {
		
		private int i;
		public ExecuteBatchSubTask(int i){
			this.i = i;
		}
		public void run(){
			ArrayList<String> batchValueList = null;
			switch(sqlkind){
			//TODO: primaryKey is null, autoGenerated primarykey(mysql)
			case INSERT:
				for(int j = 1 + i*blockSize; j <= (i+1)*blockSize; j++){
					try {
						batchValueList = arguValueListMap.get(IMDGString.BATCH_KEY + j);
						if (batchValueList == null) {
							continue;
						}
						hzExecutor.executeInsert(sqlsentence, arguNameList, batchValueList, hzrs);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				break;
			case DELETE:
				for(int j = 1 + i*blockSize; j <= (i+1)*blockSize; j++){
					try {
						batchValueList = arguValueListMap.get(IMDGString.BATCH_KEY + j);
						if (batchValueList == null) {
							break;
						}
						hzExecutor.executeDelete(sqlsentence, arguNameList, batchValueList);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				break;
			case UPDATE:
				if(setargunum == null) {
					setargunum = IMDGPreparedStatement.updateSetArguNumMap.get(sqlsentence);
				}
				if(updatesetArguList == null) {
					updatesetArguList = new ArrayList<String>(arguNameList.subList(0, setargunum));
					updatewhereArguList = new ArrayList<String>(arguNameList.subList(setargunum, arguNameList.size()));
				}
				for(int j = 1 + i*blockSize; j <= (i+1)*blockSize; j++){
					try {
						batchValueList = arguValueListMap.get(IMDGString.BATCH_KEY + j);
						if (batchValueList == null) {
							break;
						}
						ArrayList<String> updatesetValueList = new ArrayList<String>(batchValueList.subList(0, setargunum));
						ArrayList<String> updatewhereValueList = new ArrayList<String>(batchValueList.subList(setargunum, batchValueList.size()));
						hzExecutor.executeUpdate(sqlsentence, updatesetArguList, updatesetValueList, updatewhereArguList, updatewhereValueList);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				break;
			default:
				try {
					throw new SQLException("Unknow SQL Type");
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
	
}
