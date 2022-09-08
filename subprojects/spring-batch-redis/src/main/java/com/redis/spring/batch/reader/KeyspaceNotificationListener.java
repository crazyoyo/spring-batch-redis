package com.redis.spring.batch.reader;

public interface KeyspaceNotificationListener<K> {

	void notification(K notification);

}
