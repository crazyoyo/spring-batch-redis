package com.redis.spring.batch.reader;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> {

	private final Supplier<StatefulConnection<K, V>> connectionSupplier;
	private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
	public static final String DEFAULT_SCAN_MATCH = "*";
	public static final long DEFAULT_SCAN_COUNT = 1000;

	private final String match;
	private final long count;
	private final Optional<String> type;

	private ScanIterator<K> scanIterator;

	public ScanKeyItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync, String match, long count,
			Optional<String> type) {
		this.connectionSupplier = connectionSupplier;
		this.sync = sync;
		this.match = match;
		this.count = count;
		this.type = type;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (scanIterator == null) {
			KeyScanArgs args = KeyScanArgs.Builder.limit(count).match(match);
			type.ifPresent(args::type);
			scanIterator = ScanIterator.scan((RedisKeyCommands<K, V>) sync.apply(connectionSupplier.get()), args);
		}
	}

	@Override
	public synchronized K read() {
		if (scanIterator.hasNext()) {
			return scanIterator.next();
		}
		return null;
	}

	@Override
	public synchronized void close() {
		super.close();
		if (scanIterator != null) {
			scanIterator = null;
		}
	}

}
