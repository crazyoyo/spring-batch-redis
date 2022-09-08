package com.redis.spring.batch.reader;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.spring.batch.common.Utils;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> {

	private final GenericObjectPool<StatefulConnection<K, V>> connectionPool;
	private final ScanReaderOptions options;
	private ScanIterator<K> scanIterator;

	public ScanKeyItemReader(GenericObjectPool<StatefulConnection<K, V>> connectionPool, ScanReaderOptions options) {
		this.connectionPool = connectionPool;
		this.options = options;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (scanIterator == null) {
			KeyScanArgs args = KeyScanArgs.Builder.limit(options.getCount()).match(options.getMatch());
			options.getType().ifPresent(args::type);
			try (StatefulConnection<K, V> connection = connectionPool.borrowObject()) {
				scanIterator = ScanIterator.scan(Utils.sync(connection), args);
			} catch (Exception e) {
				throw new ItemStreamException("Could not get connection", e);
			}
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
