package org.springframework.batch.item.redis.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyItemReader<K, V> extends AbstractProgressReportingItemReader<K> {

    private final StatefulConnection<K, V> connection;
    private Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commandsSupplier;
    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> asyncCommandsSupplier;
    private final long commandTimeout;
    private final long scanCount;
    private final String scanMatch;
    private final int keySampleSize;
    private final Filter<K> keyFilter;
    private long size;
    private ScanIterator<K> iterator;

    public KeyItemReader(StatefulConnection<K, V> connection,
	    Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commandsSupplier,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> asyncCommandsSupplier,
	    Duration commandTimeout, long scanCount, String scanMatch, int keySampleSize, Filter<K> keyFilter) {
	setName(ClassUtils.getShortName(getClass()));
	Assert.notNull(connection, "A connection is required.");
	Assert.notNull(asyncCommandsSupplier, "A commands supplier is required.");
	Assert.notNull(commandTimeout, "Command timeout is required.");
	Assert.isTrue(scanCount > 0, "Scan count must be greater than 0.");
	Assert.notNull(scanMatch, "Scan match is required.");
	this.connection = connection;
	this.commandsSupplier = commandsSupplier;
	this.asyncCommandsSupplier = asyncCommandsSupplier;
	this.commandTimeout = commandTimeout.getSeconds();
	this.scanMatch = scanMatch;
	this.scanCount = scanCount;
	this.keySampleSize = keySampleSize;
	this.keyFilter = keyFilter;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doOpen() throws InterruptedException, ExecutionException, TimeoutException {
	if (iterator != null) {
	    return;
	}
	long dbsize = ((RedisServerCommands<K, ?>) commandsSupplier.apply(connection)).dbsize();
	BaseRedisAsyncCommands<K, V> commands = this.asyncCommandsSupplier.apply(connection);
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
	ScanArgs scanArgs = ScanArgs.Builder.limit(scanCount).match(scanMatch);
	this.iterator = ScanIterator.scan((RedisKeyCommands<K, V>) commandsSupplier.apply(connection), scanArgs);
    }

    @Override
    public Long getTotal() {
	return size;
    }

    @Override
    protected synchronized void doClose() {
	iterator = null;
    }

    @Override
    protected synchronized K doRead() {
	if (iterator.hasNext()) {
	    return iterator.next();
	}
	log.info("{} complete - {} keys read", ClassUtils.getShortName(getClass()), getCurrentItemCount());
	return null;
    }

    public static KeyItemReaderBuilder<String, String> builder() {
	return new KeyItemReaderBuilder<>(StringCodec.UTF8, b -> KeyFilter.builder().pattern(b.scanMatch).build());
    }

    @Setter
    @Accessors(fluent = true)
    public static class KeyItemReaderBuilder<K, V> extends RedisConnectionBuilder<K, V, KeyItemReaderBuilder<K, V>> {

	public static final long DEFAULT_SCAN_COUNT = 1000;
	public static final String DEFAULT_SCAN_MATCH = "*";
	public static final int DEFAULT_KEY_SAMPLE_SIZE = 100;

	private final Function<KeyItemReaderBuilder<K, V>, Filter<K>> keyFilterProvider;
	private long scanCount = DEFAULT_SCAN_COUNT;
	private String scanMatch = DEFAULT_SCAN_MATCH;
	private int keySampleSize = DEFAULT_KEY_SAMPLE_SIZE;

	public KeyItemReaderBuilder(RedisCodec<K, V> codec,
		Function<KeyItemReaderBuilder<K, V>, Filter<K>> keyFilterProvider) {
	    super(codec);
	    this.keyFilterProvider = keyFilterProvider;
	}

	public KeyItemReader<K, V> build() {
	    return new KeyItemReader<>(connection(), sync(), async(), timeout(), scanCount, scanMatch, keySampleSize,
		    keyFilterProvider.apply(this));
	}

    }

}
