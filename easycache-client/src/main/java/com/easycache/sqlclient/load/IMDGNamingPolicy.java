package com.easycache.sqlclient.load;
import net.sf.cglib.core.Predicate;

public class IMDGNamingPolicy implements net.sf.cglib.core.NamingPolicy {
	private String name = null;
	public IMDGNamingPolicy(String name) {
		this.name = name.toLowerCase();
	}
	public String getClassName(String prefix, String source, Object key, Predicate names) {
		return "com.easycache.sqlclient." + name;
	}

}
