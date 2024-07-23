package com.redis.spring.batch.item.redis.reader;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.item.redis.common.KeyEvent;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyScanNotificationItemReader<K, V> extends KeyNotificationItemReader<K, V> {

	private final KeyScanItemReader<K, V> scanReader;

	public KeyScanNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec,
			KeyScanItemReader<K, V> scanReader) {
		super(client, codec);
		this.scanReader = scanReader;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		scanReader.open(executionContext);
		super.open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		scanReader.update(executionContext);
		super.update(executionContext);
	}

	@Override
	public void close() throws ItemStreamException {
		super.close();
		scanReader.close();
	}

	@Override
	protected KeyEvent<K> doPoll(long timeout, TimeUnit unit) throws Exception {
		if (queue.isEmpty()) {
			KeyEvent<K> keyEvent = scanReader.read();
			if (keyEvent != null) {
				return keyEvent;
			}
		}
		return super.doPoll(timeout, unit);
	}

}
