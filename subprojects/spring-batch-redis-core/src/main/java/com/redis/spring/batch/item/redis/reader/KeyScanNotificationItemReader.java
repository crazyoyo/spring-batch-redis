package com.redis.spring.batch.item.redis.reader;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.support.IteratorItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyScanNotificationItemReader<K, V> extends KeyNotificationItemReader<K, V> {

	private final IteratorItemReader<K> scanReader;

	public KeyScanNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec,
			IteratorItemReader<K> scanReader) {
		super(client, codec);
		this.scanReader = scanReader;
	}

	@Override
	protected K doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		if (queue.isEmpty()) {
			K key = scanReader.read();
			if (key != null && !queue.contains(key)) {
				return key;
			}
		}
		return super.doPoll(timeout, unit);
	}

}
