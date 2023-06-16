package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.Optional;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> {

	public static final String DEFAULT_MATCH = "*";
	public static final long DEFAULT_COUNT = 1000;

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private String match = DEFAULT_MATCH;
	private long count = DEFAULT_COUNT;
	private Optional<String> type = Optional.empty();
	private StatefulRedisModulesConnection<K, V> connection;
	private Iterator<K> iterator;

	public ScanKeyItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.client = client;
		this.codec = codec;
	}

	public ScanKeyItemReader<K, V> withMatch(String match) {
		this.match = match;
		return this;
	}

	public ScanKeyItemReader<K, V> withCount(long count) {
		this.count = count;
		return this;
	}

	public ScanKeyItemReader<K, V> withType(String type) {
		return withType(Optional.of(type));
	}

	public ScanKeyItemReader<K, V> withType(Optional<String> type) {
		this.type = type;
		return this;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (connection == null) {
			connection = RedisModulesUtils.connection(client, codec);
		}
		if (iterator == null) {
			iterator = ScanIterator.scan(connection.sync(), args());
		}
	}

	@Override
	public synchronized void close() {
		super.close();
		if (connection != null) {
			connection.close();
		}
	}

	private ScanArgs args() {
		KeyScanArgs args = KeyScanArgs.Builder.limit(count).match(match);
		type.ifPresent(args::type);
		return args;
	}

	@Override
	public synchronized K read() {
		if (iterator != null && iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

}
