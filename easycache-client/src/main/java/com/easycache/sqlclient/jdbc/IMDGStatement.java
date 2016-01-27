package com.easycache.sqlclient.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.easycache.sqlclient.executor.HazelcastExecutor;
import com.easycache.sqlclient.log.IMDGLog;

public class IMDGStatement implements Statement {
	protected boolean closed = false;
	protected IMDGResultSet hzrs = null;
	protected ArrayList<IMDGResultSet> mulResultSets = null; // multiple resultset for one sql statement (not supporte yet)
	protected ArrayList<Integer> mulUpdateCount = null; // collaborate with mulResultSets for multiple-resultset situation
	protected int updateCount = -1;
	protected ArrayList<String> batchedArgs = null; // container of batched sql

	protected Connection connection = null;
	protected Statement dbst = null; // statement of DataBase
	protected boolean isReadOnly = false;

	protected int fetchSize = 0;
	protected int maxRows = 0;
	protected int timeoutInMillis = 0;
	protected SQLWarning warningChain = null;
	protected int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
	protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
	// sql parse class
	protected HazelcastExecutor hzExecutor = new HazelcastExecutor();

	// complex sql throwed into DB to execute
	protected void DBExecuteQuery(String sql) throws SQLException {
		ResultSet rsSet = dbst.executeQuery(sql);
		ResultSetMetaData rsData = rsSet.getMetaData();
		ArrayList<String> namesList = new ArrayList<String>();
		ArrayList<String> typeNameList = new ArrayList<String>();
		ArrayList<String> colsTabList = new ArrayList<String>();
		ArrayList<String> colsDBList = new ArrayList<String>();
		ArrayList<List<Object> > colsValueList = new ArrayList<List<Object> >();

		int columncount = rsData.getColumnCount();
		for (int i = 1; i <= columncount; i++) {
			String columnName = rsData.getColumnName(i);
			namesList.add(columnName);

			String className = rsData.getColumnClassName(i);
			typeNameList.add(className);

			colsTabList.add(rsData.getTableName(i));
			colsDBList.add(rsData.getCatalogName(i));
		}
//		hzrs.clear(); clear operation should been done in the caller function
//		hzrs.addColName(namesList);
		hzrs.addMetaData(namesList, typeNameList, colsTabList, colsDBList);
		while (rsSet.next()) {
			ArrayList<Object>  rowlist = new ArrayList<Object>();
			columncount = rsData.getColumnCount();
			for (int i = 1; i <= columncount; i++) {
				rowlist.add(rsSet.getObject(i));
			}
			colsValueList.add((List<Object>)rowlist);
		}
		hzrs.addRowValueData(colsValueList);
		hzrs.checkResult();
		rsSet.close();
//		System.out.println("DBExecutor: " + sql);
	}
	// DataBase Execute Update for unsupported SQL
	protected int DBExecuteUpdate(String sql) throws SQLException {
		updateCount = dbst.executeUpdate(sql);
		return updateCount;
	}
	// DataBase Execute Insert for unsupported SQL
	protected int DBExecuteInsert(String sql) throws SQLException {
		updateCount = dbst.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
		ResultSet rsSet = dbst.getGeneratedKeys();
		ResultSetMetaData rsData = rsSet.getMetaData();
		ArrayList<String> namesList = new ArrayList<String>();
		ArrayList<String> typeNameList = new ArrayList<String>();
		ArrayList<String> colsTabList = new ArrayList<String>();
		ArrayList<String> colsDBList = new ArrayList<String>();
		ArrayList<List<Object> > colsValueList = new ArrayList<List<Object> >();
		
		int columncount = rsData.getColumnCount();
		for (int i = 1; i <= columncount; i++) {
			String columnName = rsData.getColumnName(i);
			namesList.add(columnName);

			String className = rsData.getColumnClassName(i);
			typeNameList.add(className);

			colsTabList.add(rsData.getTableName(i));
			colsDBList.add(rsData.getCatalogName(i));
		}
//		hzrs.clear(); clear operation should been done in the caller function
//		hzrs.addColName(namesList);
		hzrs.addMetaData(namesList, typeNameList, colsTabList, colsDBList);
		while (rsSet.next()) {
			ArrayList<Object>  rowlist = new ArrayList<Object>();
			columncount = rsData.getColumnCount();
			for (int i = 1; i <= columncount; i++) {
				rowlist.add(rsSet.getObject(i));
			}
			colsValueList.add((List<Object>)rowlist);
		}
		hzrs.addRowValueData(colsValueList);
		hzrs.checkResult();
		rsSet.close();
		return 0;
	}
	// DataBase Execute Delete for unsupported SQL
	protected int DBExecuteDelete(String sql) throws SQLException {
		updateCount = dbst.executeUpdate(sql);
		return updateCount;
	}
	
	public IMDGStatement(Connection c, Statement statement) throws SQLException {
		this.connection = c;
		this.dbst = statement;
	}

	public void addBatch(String sql) throws SQLException {
		if (this.batchedArgs == null) {
			this.batchedArgs = new ArrayList<String>();
		}

		if (sql != null) {
			this.batchedArgs.add(sql);
		}
	}

	public void cancel() throws SQLException {
		// hard to supporte
		throw new SQLException("No Supported");
	}

	protected void checkClosed() throws SQLException {
		if (this.closed) {
			throw new SQLException("SQL_STATE_CONNECTION_NOT_OPEN");
		}
	}

	void checkReadOnly() throws SQLException {
		if (isReadOnly)
			throw new SQLException("Read Only Statement");
	}

	protected void clear() throws SQLException {
		if (hzrs != null) {
			hzrs.clear();
		}
		if (mulResultSets != null) {
			mulResultSets.clear();
		}
		if (mulUpdateCount != null) {
			mulUpdateCount.clear();
		}
		if (batchedArgs != null) {
			batchedArgs.clear();
		}
		updateCount = -1;
	}

	public void clearBatch() throws SQLException {
		if (this.batchedArgs != null) {
			this.batchedArgs.clear();
		}
	}

	public void clearWarnings() throws SQLException {
		warningChain = null;
	}

	public void close() throws SQLException {
		closed = true;
		if (dbst != null) {
			dbst.close();
			dbst = null;
		}
		// 不需要ArrayList的clear操作
//		clear();
		hzrs = null;
		mulResultSets = null;
		mulUpdateCount = null;
		batchedArgs = null;
		updateCount = -1;
	}

	public boolean execute(String sql) throws SQLException {
		// MySQL Connector: this function can return more than one resultset
		// they can be fetched by invoking getMoreResult()
		// not supported yet now
		// attention: before doing any insert delete update or select operation,
		// must clear the old data
		return true;
	}

	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		// return primary key default
		IMDGLog.stmtFuncLog("execute(String sql, int autoGeneratedKeys)");
		return execute(sql);
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		IMDGLog.stmtFuncLog("execute(String sql, int[] columnIndexes)");
		IMDGLog.showStmtSql(sql);
		// too complex
		return false;
	}

	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		IMDGLog.stmtFuncLog("execute(String sql, String[] columnNames)");
		IMDGLog.showStmtSql(sql);
		// 数组指示的目标表中指定的自动生成的列应该可用于获取
		return false;
	}

	public int[] executeBatch() throws SQLException {
		// 执行executeBatch将返回一个int[]的数组，因为使用Statement执行DDL、DML都将返回一个int的值，
		// 而执行多条DDL、DML也将返回一个int数组。批量更新中不允许出现select查询语句，一旦出现程序将出现异常。
		// 如果要批量更新正确、批量完成，需要用单个事务，如果批量更新过程中有失败，则需要用事务回滚到原始状态。
		// 如果要达到这样的效果，需要关闭事务的自动提交，当批量更新完成再提交事务，如果出现异常将回滚事务。
		// 然后将连接恢复成自动提交模式。
		checkClosed();
		Connection locallyScopedConn = this.connection;
		if (locallyScopedConn.isReadOnly()) {
			throw new SQLException("SQL_STATE_ILLEGAL_ARGUMENT");
		}

		if (this.batchedArgs == null || this.batchedArgs.size() == 0) {
			return new int[0];
		}
		int[] resultset = new int[batchedArgs.size()];
		int length = resultset.length;
		for (int i = 0; i < length; i++) {
			resultset[i] = executeUpdate(batchedArgs.get(i));
		}
		return resultset;
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		return hzrs;
	}

	public int executeUpdate(String sql) throws SQLException {
		return updateCount;
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		IMDGLog.stmtFuncLog("executeUpdate(String sql, int autoGeneratedKeys) ");
		return executeUpdate(sql);
	}

	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		// 这个太复杂了，下一版再说
		IMDGLog.stmtFuncLog("executeUpdate(String sql, int[] columnIndexes) ");
		IMDGLog.showStmtSql(sql);
		return 0;
	}

	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		// 这个太复杂了，下一版再说
		IMDGLog.stmtFuncLog("executeUpdate(String sql, int[] columnIndexes) ");
		IMDGLog.showStmtSql(sql);
		return 0;
	}

	public Connection getConnection() throws SQLException {
		return this.connection;
	}

	public int getFetchDirection() throws SQLException {
		return java.sql.ResultSet.FETCH_FORWARD;
	}

	public int getFetchSize() throws SQLException {
		return this.fetchSize;
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		// 获取由于执行此 Statement 对象而创建的所有自动生成的键.
		// 如果此 Statement 对象没有生成任何键，则返回空的 ResultSet 对象。
		// 注：如果未指定表示自动生成键的列，则 JDBC 驱动程序实现将确定最能表示自动生成键的列。
		// 这个功能要求对于插入操作新添加的列需要返回这些列的主键，然后构造ResultSet
		// 这里并没有设计完整，还需要想办法将key的值传递给这些对象
		// 目前这个版本只支持单列主键的情况
		return hzrs;
	}

	public int getMaxFieldSize() throws SQLException {
		// For Version 2
		return fetchSize;
	}

	public int getMaxRows() throws SQLException {
		return this.maxRows;
	}

	public boolean getMoreResults() throws SQLException {
		// 这个不能够支持
		throw new SQLException("No Supported");
	}

	public boolean getMoreResults(int current) throws SQLException {
		// 对应excute()返回多个resultsetet的情形
		// 如果返回的是resultsetet这里返回true，如果没有结果或者是进行的更新操作则返回false
		// 需要通过getUpdateCount() == -1 来判断
		throw new SQLException("No Supported");
		// 这个功能没有想好 有点麻烦啊~~~
	}

	public int getQueryTimeout() throws SQLException {
		return timeoutInMillis / 1000;
	}

	public ResultSet getResultSet() throws SQLException {
		return hzrs;
	}

	public int getResultSetConcurrency() throws SQLException {
		return this.resultSetConcurrency;
	}

	public int getResultSetHoldability() throws SQLException {
		// 未实现
		return 0;
	}

	public int getResultSetType() throws SQLException {
		return this.resultSetType;
	}

	public int getUpdateCount() throws SQLException {
		// 需要在更新操作时更新 updateCount;
		return updateCount;
	}

	public SQLWarning getWarnings() throws SQLException {
		return this.warningChain;
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public boolean isPoolable() throws SQLException {
		// 未实现
		return false;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	public void setCursorName(String name) throws SQLException {
		throw new SQLException("No Supported");
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		// 禁止转移字符处理
		throw new SQLException("forbid escape process");
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
		if (((rows < 0) && (rows != Integer.MIN_VALUE))
				|| ((this.maxRows != 0) && (this.maxRows != -1) && (rows > this
						.getMaxRows()))) {
			throw new SQLException("SQL_STATE_ILLEGAL_ARGUMENT");
		}
		this.fetchSize = rows;
	}

	public void setMaxFieldSize(int max) throws SQLException {
		// For Version 2
		if (max < 0) {
			throw new SQLException("ilegal argument");
		}
		fetchSize = max;
	}

	public void setMaxRows(int max) throws SQLException {
		if (max <= 0) {
			throw new SQLException("ilegal argument");
		}
		maxRows = max;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		// 未实现

	}

	public void setQueryTimeout(int seconds) throws SQLException {
		if (seconds < 0) {
			throw new SQLException("ilegal argument");
		}
		timeoutInMillis = seconds * 1000;
	}

	void setReadOnly(boolean isReadOnly) {
		this.isReadOnly = isReadOnly;
	}

	void setResultSetConcurrency(int resultSetConcurrency) {
		this.resultSetConcurrency = resultSetConcurrency;
	}

	void setResultSetType(int resultSetType) {
		this.resultSetType = resultSetType;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
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
