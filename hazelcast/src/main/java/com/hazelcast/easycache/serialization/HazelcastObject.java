package com.hazelcast.easycache.serialization;

import java.lang.reflect.Field;

public class HazelcastObject {
	private String id;
	public String getId() {
		return id;
	}
		
	public void setId(String id) {
		this.id = id;
	}
	
//	public void setImdgResultInfo(boolean isImdgResult){
//		this.isImdgResult = isImdgResult;
//	}
//	
	public HazelcastObject copy() {
		// yanshi.wang
		// setAccessible may be a problem
		HazelcastObject obj = null;
		try {
			obj = this.getClass().newInstance();
//			System.out.println("this.classname:" + this.getClass().getName());
			Field[] fromFields = this.getClass().getDeclaredFields();
			for (Field field : fromFields) {
				field.setAccessible(true);
				//jiang yong 2014-11-06
				//下面这句 不能实现对list, map等数据类型的深复制
				field.set(obj, field.get(this));
			}
			//jiang yong 2014-11-04
			//按照上述方法拷贝HazelcastObject id为null，加入以下代码
			obj.id = this.id;
			//done
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
		return obj;
	}
	
//	public static boolean isValueType(Object obj){
//		String className = obj.getClass().getName();
//		if(className.equalsIgnoreCase("java.lang.byte")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.short")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.Integer")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.String")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.float")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.double")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.boolean")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.character")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.lang.String")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.sql.Date")){
//			return true;
//		}
//		if(className.equalsIgnoreCase("java.sql.TimeStamp")){
//			return true;
//		}
//		return false;
//	}
//	public static List<Field> getAllFields(Object obj) {
//
//		List<Field> fields = new ArrayList<Field>();
//		Class<?> type = obj.getClass();
//		while(type != null){
//			for (Field field : type.getDeclaredFields()) {
//				fields.add(field);
//			}
//			type = type.getSuperclass();
//		}
//		return fields;
//
//	}
//	public static Object copy(Object obj) {
//		Object hzObj = null;
//		if(obj == null){
//			return hzObj;
//		}
//		if(isValueType(obj)){
//			hzObj = obj;
//		}
//		else{
//			try {
//				hzObj = obj.getClass().newInstance();
////				System.out.println("this.classname:" + this.getClass().getName());
//				List<Field> fromFields = getAllFields(obj);
//				for (Field field : fromFields) {
//					field.setAccessible(true);
//					field.set(hzObj, copy(field.get(obj)));
//				}
//			} catch (InstantiationException e) {
//				e.printStackTrace();
//			} catch (IllegalAccessException e) {
//				e.printStackTrace();
//			} catch (SecurityException e) {
//				e.printStackTrace();
//			} catch (Exception e){
//				e.printStackTrace();
//			}
//		}
//		return hzObj;
//	}
}