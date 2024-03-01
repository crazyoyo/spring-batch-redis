package com.redis.spring.batch.reader;

public interface KeyspaceNotificationListener extends AutoCloseable {

	void start();

	default void stop() {
		close();
	}

	@Override
	void close();

}
