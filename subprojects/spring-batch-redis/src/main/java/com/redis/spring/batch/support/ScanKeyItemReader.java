package com.redis.spring.batch.support;

import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;

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

	private String match = DEFAULT_SCAN_MATCH;
	private long count = DEFAULT_SCAN_COUNT;
	private String type;

	private StatefulConnection<K, V> connection;
	private ScanIterator<K> scanIterator;

	public ScanKeyItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync) {
		this.connectionSupplier = connectionSupplier;
		this.sync = sync;
	}

	public void setMatch(String match) {
		Assert.notNull(match, "Scan match cannot be null");
		this.match = match;
	}

	public void setCount(long count) {
		Utils.assertPositive(count, "Scan count");
		this.count = count;
	}

	public void setType(String type) {
		this.type = type;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (connection == null) {
			connection = connectionSupplier.get();
			KeyScanArgs args = KeyScanArgs.Builder.limit(count);
			if (match != null) {
				args.match(match);
			}
			if (type != null) {
				args.type(type);
			}
			scanIterator = ScanIterator.scan((RedisKeyCommands<K, V>) sync.apply(connection), args);
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
		if (connection != null) {
			connection.close();
		}
		super.close();
	}

}
