package com.easycache.sqlclient;

import java.util.HashMap;
import java.util.Map;

import net.sf.cglib.beans.BeanGenerator;

public class BeanGeneratorFactory {	
	private static Map<String, BeanGenerator> beanGeneratorFactory = new HashMap<String, BeanGenerator>();
	
	public BeanGenerator getBeanGenerator(String tableName){
		return beanGeneratorFactory.get(tableName);
	}
	
	public void setBeanGenerator(String tableName, BeanGenerator beanGenerator){
		if(beanGeneratorFactory.get(tableName) == null){
			beanGeneratorFactory.put(tableName, beanGenerator);
		}
	}
}
