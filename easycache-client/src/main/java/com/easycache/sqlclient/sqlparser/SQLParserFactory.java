package com.easycache.sqlclient.sqlparser;

import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.statement.Statement;

public class SQLParserFactory {
	private static Map<String, Statement> parserMap= new HashMap<String, Statement>();

	public static Statement getStatement(String sql){
		return parserMap.get(sql);
	}
	
	public static void setStatement(String sql, Statement statement) {
		parserMap.put(sql, statement);
	}
}
