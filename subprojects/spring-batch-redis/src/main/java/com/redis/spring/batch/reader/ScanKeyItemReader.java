package com.redis.spring.batch.reader;

import java.util.Iterator;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> implements AutoCloseable {

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
		KeyScanArgs args = KeyScanArgs.Builder.limit(options.getCount()).match(options.getMatch());
		options.getType().ifPresent(args::type);
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
