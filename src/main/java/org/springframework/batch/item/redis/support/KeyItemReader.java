package org.springframework.batch.item.redis.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<K> implements ProgressReporter {

    private final StatefulConnection<K, V> connection;
    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commandsSupplier;
    private final long commandTimeout;
    private final long scanCount;
    private final String scanMatch;
    private final int keySampleSize;
    private final Filter<K> keyFilter;
    private long size;
    private ScanArgs scanArgs;
    private Iterator<K> keyIterator;
    private KeyScanCursor<K> cursor;

    public KeyItemReader(StatefulConnection<K, V> connection,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commandsSupplier, Duration commandTimeout,
	    long scanCount, String scanMatch, int keySampleSize, Filter<K> keyFilter) {
	setName(ClassUtils.getShortName(getClass()));
	Assert.notNull(connection, "A connection is required.");
	Assert.notNull(commandsSupplier, "A commands supplier is required.");
	Assert.notNull(commandTimeout, "Command timeout is required.");
	Assert.isTrue(scanCount > 0, "Scan count must be greater than 0.");
	Assert.notNull(scanMatch, "Scan match is required.");
	this.connection = connection;
	this.commandsSupplier = commandsSupplier;
	this.commandTimeout = commandTimeout.getSeconds();
	this.scanMatch = scanMatch;
	this.scanCount = scanCount;
	this.keySampleSize = keySampleSize;
	this.keyFilter = keyFilter;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doOpen() throws InterruptedException, ExecutionException, TimeoutException {
	if (cursor != null) {
	    return;
	}
	BaseRedisAsyncCommands<K, V> commands = this.commandsSupplier.apply(connection);
	long dbsize = ((RedisServerAsyncCommands<K, ?>) commands).dbsize().get(commandTimeout, TimeUnit.SECONDS);
	commands.setAutoFlushCommands(false);
	List<RedisFuture<K>> futures = new ArrayList<>(keySampleSize);
	// rough estimate of keys matching pattern
	for (int index = 0; index < keySampleSize; index++) {
	    futures.add(((RedisKeyAsyncCommands<K, V>) commands).randomkey());
	}
	commands.flushCommands();
	commands.setAutoFlushCommands(true);
	int matchCount = 0;
	for (RedisFuture<K> future : futures) {
	    try {
		K key = future.get(commandTimeout, TimeUnit.SECONDS);
		if (keyFilter.accept(key)) {
		    matchCount++;
		}
	    } catch (ExecutionException e) {
		log.error("Could not get random key", e.getCause());
	    } catch (TimeoutException e) {
		log.error("Command timed out", e);
	    }
	}
	double rate = (double) matchCount / keySampleSize;
	size = Math.round(dbsize * rate);
	scanArgs = ScanArgs.Builder.limit(scanCount).match(scanMatch);
	cursor = ((RedisKeyAsyncCommands<K, V>) commands).scan(scanArgs).get(commandTimeout, TimeUnit.SECONDS);
	keyIterator = cursor.getKeys().iterator();
    }

    @Override
    public Long getTotal() {
	return size;
    }

    @Override
    public long getDone() {
	return getCurrentItemCount();
    }

    @Override
    protected synchronized void doClose() {
	if (cursor == null) {
	    return;
	}
	cursor = null;
	keyIterator = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized K doRead() throws Exception {
	while (!(keyIterator.hasNext() || cursor.isFinished())) {
	    cursor = ((RedisKeyAsyncCommands<K, V>) commandsSupplier.apply(connection)).scan(cursor, scanArgs)
		    .get(commandTimeout, TimeUnit.SECONDS);
	    keyIterator = cursor.getKeys().iterator();
	}
	if (keyIterator.hasNext()) {
	    return keyIterator.next();
	}
	if (cursor.isFinished()) {
	    return null;
	}
	throw new IllegalStateException("No more keys but cursor not finished");
    }

}
