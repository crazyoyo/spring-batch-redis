package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;

import java.util.function.Supplier;

public class ConnectionPoolItemStream extends ItemStreamSupport {

    private final Supplier<StatefulConnection<String, String>> connectionSupplier;
    private final GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig;
    protected GenericObjectPool<StatefulConnection<String, String>> pool;

    protected ConnectionPoolItemStream(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) {
        Assert.notNull(connectionSupplier, "A connection supplier is required");
        Assert.notNull(poolConfig, "A connection pool config is required");
        this.connectionSupplier = connectionSupplier;
        this.poolConfig = poolConfig;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        if (pool == null) {
            this.pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // do nothing
    }

    @Override
    public synchronized void close() {
        if (pool != null) {
            pool.close();
        }
    }

}
