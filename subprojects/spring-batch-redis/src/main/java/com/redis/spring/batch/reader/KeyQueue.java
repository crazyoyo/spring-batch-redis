package com.redis.spring.batch.reader;

import java.util.concurrent.TimeUnit;

public interface KeyQueue<K> {

	void offer(K key);

	K poll(long timeout, TimeUnit unit) throws InterruptedException;

	void clear();

}
