package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public abstract class AbstractKeyValueItemProcessor<K, V, T> implements ItemProcessor<List<? extends K>, List<T>> {

    private final GenericObjectPool<? extends StatefulConnection<K, V>> pool;

    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands;

    private final long commandTimeout;

    protected AbstractKeyValueItemProcessor(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout) {
	this.pool = pool;
	this.commands = commands;
	this.commandTimeout = commandTimeout.getSeconds();
    }

    @Override
    public List<T> process(List<? extends K> items) throws Exception {
	try (StatefulConnection<K, V> connection = pool.borrowObject()) {
	    BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
	    commands.setAutoFlushCommands(false);
	    try {
		return values(items, commands);
	    } finally {
		commands.setAutoFlushCommands(true);
	    }
	}
    }

    protected abstract List<T> values(List<? extends K> items, BaseRedisAsyncCommands<K, V> commands);

    protected <F> F get(RedisFuture<F> future) throws InterruptedException, ExecutionException, TimeoutException {
	if (future == null) {
	    return null;
	}
	return future.get(commandTimeout, TimeUnit.SECONDS);
    }

    protected long getTtl(RedisFuture<Long> future) throws InterruptedException, ExecutionException, TimeoutException {
	Long ttl = get(future);
	if (ttl == null) {
	    return 0;
	}
	return ttl;
    }

}
