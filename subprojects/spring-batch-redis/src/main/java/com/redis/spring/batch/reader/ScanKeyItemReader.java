package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.function.Supplier;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> implements Openable {

	private final Supplier<StatefulConnection<K, V>> connectionSupplier;

	private ScanOptions options = ScanOptions.builder().build();
	private Iterator<K> iterator;

	public ScanKeyItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	public ScanOptions getOptions() {
		return options;
	}

	public void setOptions(ScanOptions options) {
		this.options = options;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (iterator == null) {
			iterator = ScanIterator.scan(Utils.sync(connectionSupplier.get()), args());
		}
	}

	@Override
	public boolean isOpen() {
		return iterator != null;
	}

	@Override
	public synchronized void close() {
		super.close();
		if (iterator != null) {
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
			K key = iterator.next();
			return key;
		}
		return null;
	}

}
