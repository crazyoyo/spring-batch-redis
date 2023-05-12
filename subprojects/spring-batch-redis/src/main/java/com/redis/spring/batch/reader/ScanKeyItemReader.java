package com.redis.spring.batch.reader;

import java.util.Iterator;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.Openable;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> implements AutoCloseable, Openable {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final ScanOptions options;
	private StatefulRedisModulesConnection<K, V> connection;
	private Iterator<K> iterator;

	public ScanKeyItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, ScanOptions options) {
		this.client = client;
		this.codec = codec;
		this.options = options;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (!isOpen()) {
			connection = RedisModulesUtils.connection(client, codec);
			iterator = ScanIterator.scan(connection.sync(), args());
		}
	}

	@Override
	public void close() {
		super.close();
		if (isOpen()) {
			iterator = null;
			connection.close();
		}
	}

	@Override
	public boolean isOpen() {
		return iterator != null;
	}

	private ScanArgs args() {
		KeyScanArgs args = KeyScanArgs.Builder.limit(options.getCount()).match(options.getMatch());
		options.getType().ifPresent(args::type);
		return args;
	}

	@Override
	public synchronized K read() {
		if (isOpen() && iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

}
