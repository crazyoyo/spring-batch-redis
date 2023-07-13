package com.redis.spring.batch.reader;

import java.util.Iterator;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> implements KeyItemReader<K> {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private ScanOptions scanOptions = ScanOptions.builder().build();
	private Iterator<K> iterator;

	private StatefulRedisModulesConnection<K, V> connection;

	public ScanKeyItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.client = client;
		this.codec = codec;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public ScanOptions getScanOptions() {
		return scanOptions;
	}

	public void setScanOptions(ScanOptions options) {
		this.scanOptions = options;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (!isOpen()) {
			doOpen();
		}
	}

	@SuppressWarnings("unchecked")
	private void doOpen() {
		connection = RedisModulesUtils.connection(client, codec);
		if (connection instanceof StatefulRedisClusterConnection) {
			StatefulRedisClusterConnection<K, V> clusterConnection = (StatefulRedisClusterConnection<K, V>) connection;
			scanOptions.getReadFrom().ifPresent(clusterConnection::setReadFrom);
		}
		iterator = ScanIterator.scan(Utils.sync(connection), args());
	}

	public boolean isOpen() {
		return iterator != null;
	}

	@Override
	public synchronized void close() {
		if (isOpen()) {
			doClose();
		}
		super.close();
	}

	private void doClose() {
		connection.close();
		connection = null;
		iterator = null;
	}

	private ScanArgs args() {
		KeyScanArgs args = KeyScanArgs.Builder.limit(scanOptions.getCount()).match(scanOptions.getMatch());
		scanOptions.getType().ifPresent(args::type);
		return args;
	}

	@Override
	public synchronized K read() {
		if (iterator != null && iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

	public static class Builder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		private ScanOptions scanOptions = ScanOptions.builder().build();

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public Builder<K, V> scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return this;
		}

		public ScanKeyItemReader<K, V> build() {
			ScanKeyItemReader<K, V> reader = new ScanKeyItemReader<>(client, codec);
			reader.setScanOptions(scanOptions);
			return reader;
		}

	}

}
