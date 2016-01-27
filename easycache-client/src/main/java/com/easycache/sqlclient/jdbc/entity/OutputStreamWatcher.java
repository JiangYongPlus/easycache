package com.easycache.sqlclient.jdbc.entity;

public interface OutputStreamWatcher {
	void streamClosed(WatchableOutputStream out);
}
