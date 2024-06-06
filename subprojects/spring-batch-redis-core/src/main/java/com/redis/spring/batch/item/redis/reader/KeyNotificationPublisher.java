package com.redis.spring.batch.item.redis.reader;

public interface KeyNotificationPublisher extends AutoCloseable {

	void open();

	@Override
	void close();

}
