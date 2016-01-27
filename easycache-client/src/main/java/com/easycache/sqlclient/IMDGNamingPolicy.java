package com.easycache.sqlclient;

import net.sf.cglib.core.NamingPolicy;
import net.sf.cglib.core.Predicate;

public class IMDGNamingPolicy implements NamingPolicy {
	
	private String tableName;
	public IMDGNamingPolicy(String name) {
		tableName = name;
	}
	public String getClassName(String prefix, String source, Object key, Predicate names) {
		return tableName;
	}

}
