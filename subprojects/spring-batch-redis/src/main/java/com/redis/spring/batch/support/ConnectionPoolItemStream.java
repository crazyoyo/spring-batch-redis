package com.redis.spring.batch.support;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.support.ConnectionPoolSupport;

public class ConnectionPoolItemStream<K, V> extends ItemStreamSupport {

	private static final Logger log = LoggerFactory.getLogger(ConnectionPoolItemStream.class);

	private final Supplier<StatefulConnection<K, V>> connectionSupplier;
	private final GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig;
	protected GenericObjectPool<StatefulConnection<K, V>> pool;

	protected ConnectionPoolItemStream(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig) {
		Assert.notNull(connectionSupplier, "A connection supplier is required");
		Assert.notNull(poolConfig, "A connection pool config is required");
		this.connectionSupplier = connectionSupplier;
		this.poolConfig = poolConfig;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (pool == null) {
			log.debug("Creating connection pool");
			this.pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig);
		}
	}

	public boolean isClosed() {
		return pool == null;
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		// do nothing
	}

	@Override
	public synchronized void close() {
		if (pool != null) {
			log.debug("Closing connection pool");
			pool.close();
			pool = null;
		}
	}

}
