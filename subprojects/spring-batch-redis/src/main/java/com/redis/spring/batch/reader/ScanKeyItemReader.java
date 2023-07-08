package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.Optional;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> implements Openable {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final Optional<ReadFrom> readFrom;

	private ScanOptions options = ScanOptions.builder().build();
	private Iterator<K> iterator;

	private StatefulRedisModulesConnection<K, V> connection;

	public ScanKeyItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this(client, codec, Optional.empty());
	}

	public ScanKeyItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, Optional<ReadFrom> readFrom) {
		this.client = client;
		this.codec = codec;
		this.readFrom = readFrom;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public ScanOptions getOptions() {
		return options;
	}

	public void setOptions(ScanOptions options) {
		this.options = options;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (connection == null) {
			connection = RedisModulesUtils.connection(client, codec);
			if (readFrom.isPresent() && connection instanceof StatefulRedisClusterConnection) {
				((StatefulRedisClusterConnection<K, V>) connection).setReadFrom(readFrom.get());
			}
			iterator = ScanIterator.scan(Utils.sync(connection), args());
		}
	}

	@Override
	public boolean isOpen() {
		return iterator != null;
	}

	@Override
	public synchronized void close() {
		super.close();
		if (connection != null) {
			connection.close();
			iterator = null;
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
