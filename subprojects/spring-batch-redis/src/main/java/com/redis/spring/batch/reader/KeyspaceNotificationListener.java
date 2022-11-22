package com.redis.spring.batch.reader;

public interface KeyspaceNotificationListener<K> {

	boolean notification(K notification);

}
