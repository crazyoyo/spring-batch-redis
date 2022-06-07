package com.redis.spring.batch.reader;

import java.util.Optional;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.AbstractValueReader.ValueReaderFactory;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class ScanRedisItemReaderBuilder<K, V, T extends KeyValue<K, ?>>
		extends RedisItemReaderBuilder<K, V, T, ScanRedisItemReaderBuilder<K, V, T>> {

	private String match = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
	private long count = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
	private Optional<String> type = Optional.empty();

	public ScanRedisItemReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
			ValueReaderFactory<K, V, T> valueReaderFactory) {
		super(client, codec, valueReaderFactory);
	}

	public ScanRedisItemReaderBuilder<K, V, T> match(String match) {
		this.match = match;
		return this;
	}

	public ScanRedisItemReaderBuilder<K, V, T> count(long count) {
		this.count = count;
		return this;
	}

	public ScanRedisItemReaderBuilder<K, V, T> type(String type) {
		this.type = Optional.of(type);
		return this;
	}

	public RedisItemReader<K, T> build() throws Exception {
		ScanKeyItemReader<K, V> keyReader = new ScanKeyItemReader<>(connectionSupplier(), sync());
		keyReader.setCount(count);
		keyReader.setMatch(match);
		keyReader.setType(type);
		return configure(new RedisItemReader<>(jobRunner(), keyReader, valueReader()));
	}

	public LiveRedisItemReaderBuilder<K, V, T> live() {
		LiveRedisItemReaderBuilder<K, V, T> live = new LiveRedisItemReaderBuilder<>(client, codec, valueReaderFactory);
		live.keyPatterns(match);
		live.jobRunner(jobRunner);
		live.chunkSize(chunkSize);
		live.threads(threads);
		live.valueQueueCapacity(valueQueueCapacity);
		live.queuePollTimeout(queuePollTimeout);
		live.skipPolicy(skipPolicy);
		return live;
	}

}