package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.spring.batch.common.Utils;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> {

	public static final String MATCH_ALL = "*";
	public static final String DEFAULT_MATCH = MATCH_ALL;
	public static final long DEFAULT_COUNT = 1000;

	private final Supplier<StatefulConnection<K, V>> connectionSupplier;
	private String match = DEFAULT_MATCH;
	private long count = DEFAULT_COUNT;
	private Optional<String> type = Optional.empty();
	private Iterator<K> iterator;

	public ScanKeyItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public void setType(String type) {
		setType(Optional.of(type));
	}

	public void setType(Optional<String> type) {
		this.type = type;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (iterator == null) {
			iterator = ScanIterator.scan(Utils.sync(connectionSupplier.get()), args());
		}
	}

	@Override
	public synchronized void close() {
		super.close();
		if (iterator != null) {
			iterator = null;
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
