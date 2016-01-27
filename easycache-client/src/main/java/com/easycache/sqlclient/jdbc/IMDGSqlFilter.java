/*
 * jiang yong 2015.01-2015.02
 * IMDGSqlFilter.java
 * judge if the sql is supported or not, if not, filter the sql.
 */
package com.easycache.sqlclient.jdbc;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import com.easycache.sqlclient.sqlparser.JSqlParserExecutor;
import com.easycache.sqlclient.sqlparser.SQLParserFactory;
import com.easycache.sqlclient.type.SqlKind;

public class IMDGSqlFilter {
	
	private static String[] sqlFilterArray;
	private static Set<String> sqlToCacheSet = new HashSet<String>();
	private static boolean cacheSwitch = true;
	private static boolean pstOptimization = true;
	private static CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private JSqlParserExecutor jspExecutor = new JSqlParserExecutor();
	private SqlKind kind;
	
	public static void setSqlFilter(String[] selectFilterArray, String[] insertFilterArray, String[] deleteFilterArray, String[] updateFilterArray){
		IMDGSqlFilter.sqlFilterArray = selectFilterArray;
	}
	
	public SqlKind getSqlKind() {
		return this.kind;
	}
	
	public boolean isSupportedSql(String sql) throws SQLException {
		if (!this.isArtificialSupported(sql) || !this.isParserSupported(sql)) {
			return false;
		}
		return true;
	}
	
	public boolean isArtificialSupported(String sql) {
		String tempSql = sql.toLowerCase();
		if (sqlFilterArray != null) {
			for (String sqlSentence : sqlFilterArray) {
				if (tempSql.indexOf(sqlSentence) != -1) {
					return false;
				}
			}
		}
		if (tempSql.indexOf("order by") != -1) {
			return false;
		}
		if (tempSql.indexOf("group by") != -1) {
			return false;
		}
		if (tempSql.indexOf(" count(") != -1 || sql.indexOf(" count ") != -1) {
			return false;
		}
		if (tempSql.indexOf(" top ") != -1) {
			return false;
		}
		if (tempSql.indexOf(" limit ") != -1) {
			return false;
		}
		if (tempSql.indexOf(" as ") != -1) {
			return false;
		}
		if (tempSql.indexOf("rownum") != -1) {
			return false;
		}
		if (tempSql.indexOf("like") != -1) {
			return false;
		}
		if (tempSql.indexOf(" max( ")!=-1){
			return false;
		}
		if (tempSql.indexOf(" min( ")!=-1) {
			return false;
		}		
		if (tempSql.indexOf(" distinct ")!=-1) {
			return false;
		}			
		if (tempSql.indexOf(" case ")!=-1) {
			return false;
		}
		if (tempSql.indexOf(" union ")!=-1) {
			return false;
		}
		return true;
	}
	
	public boolean isParserSupported(String sql) {
		try {
			net.sf.jsqlparser.statement.Statement statement = SQLParserFactory.getStatement(sql);
			if (statement == null) {
				statement = parserManager.parse(new StringReader(sql));
				SQLParserFactory.setStatement(sql, statement);
			}
			if (statement instanceof Select) {
				this.kind = SqlKind.SELECT;
				return jspExecutor.executeSelectPre(sql, statement);
			} else if (statement instanceof Insert) {
				this.kind = SqlKind.INSERT;
				return jspExecutor.executeInsertPre(sql, statement);
			} else if (statement instanceof Delete) {
				this.kind = SqlKind.DELETE;
				return jspExecutor.executeDeletePre(sql, statement);
			} else if (statement instanceof Update) {
				this.kind = SqlKind.UPDATE;
				return jspExecutor.executeUpdatePre(sql, statement);
			} else {
				throw new Exception("unknow sql type:" + sql);
			}
		} catch (JSQLParserException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public static void setSqlToCache(String[] sqlToCacheArray){
		for(String sql : sqlToCacheArray){
			sqlToCacheSet.add(sql);
		}
	}
	
	public static void setCacheSwitch(boolean cacheSwitch) {
		IMDGSqlFilter.cacheSwitch = cacheSwitch;
	}
	
	public boolean getCacheSwitch(){
		return IMDGSqlFilter.cacheSwitch;
	}
	
	public static void setPstOptimization(boolean pstOptimization) {
		IMDGSqlFilter.pstOptimization = pstOptimization;
	}
	
	public boolean getPstOptimization() {
		return IMDGSqlFilter.pstOptimization;
	}
	
	public boolean supportedQueryResultCache(String sql) throws SQLException {
		if(sqlToCacheSet != null){
			return sqlToCacheSet.contains(sql.toLowerCase());
		}
		return false;
	}	
	
}
