package com.easycache.sqlclient.sqlparser;

public abstract class Predicate {
	protected int priority = -1;
	protected int subQueryNumber = 0;
	protected boolean active = true;
	public boolean isActive(){
		return active;
	}
	public void setActive(){
		active = true;
	}
	public void setInactive(){
		active = false;
	}
	abstract public Predicate duplicate();
}
