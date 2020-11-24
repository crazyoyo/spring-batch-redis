package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Pattern;

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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyItemReader extends AbstractProgressReportingItemReader<String> {

	private final StatefulConnection<String, String> connection;
	private Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> commandsSupplier;
	private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncCommandsSupplier;
	private final long commandTimeout;
	private final long scanCount;
	private final String scanMatch;
	private final int keySampleSize;
	private final Pattern pattern;
	private ScanIterator<String> iterator;

	public KeyItemReader(StatefulConnection<String, String> connection,
			Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> commandsSupplier,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncCommandsSupplier,
			long commandTimeout, long scanCount, String scanMatch, int keySampleSize) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(connection, "A connection is required.");
		Assert.notNull(asyncCommandsSupplier, "A commands supplier is required.");
		Assert.notNull(commandTimeout, "Command timeout is required.");
		Assert.isTrue(scanCount > 0, "Scan count must be greater than 0.");
		Assert.notNull(scanMatch, "Scan match is required.");
		this.connection = connection;
		this.commandsSupplier = commandsSupplier;
		this.asyncCommandsSupplier = asyncCommandsSupplier;
		this.commandTimeout = commandTimeout;
		this.scanMatch = scanMatch;
		this.scanCount = scanCount;
		this.keySampleSize = keySampleSize;
		this.pattern = Pattern.compile(GlobToRegexConverter.convert(scanMatch));
	}

	@Override
	@SuppressWarnings("unchecked")
	protected synchronized void doOpen() throws InterruptedException, ExecutionException, TimeoutException {
		if (iterator != null) {
			return;
		}
		long dbsize = ((RedisServerCommands<String, String>) commandsSupplier.apply(connection)).dbsize();
		BaseRedisAsyncCommands<String, String> commands = this.asyncCommandsSupplier.apply(connection);
		commands.setAutoFlushCommands(false);
		List<RedisFuture<String>> futures = new ArrayList<>(keySampleSize);
		// rough estimate of keys matching pattern
		for (int index = 0; index < keySampleSize; index++) {
			futures.add(((RedisKeyAsyncCommands<String, String>) commands).randomkey());
		}
		commands.flushCommands();
		commands.setAutoFlushCommands(true);
		int matchCount = 0;
		for (RedisFuture<String> future : futures) {
			try {
				String key = future.get(commandTimeout, TimeUnit.SECONDS);
				if (key == null) {
					continue;
				}
				if (pattern.matcher(key).matches()) {
					matchCount++;
				}
			} catch (ExecutionException e) {
				log.error("Could not get random key", e.getCause());
			} catch (TimeoutException e) {
				log.error("Command timed out", e);
			}
		}
		float rate = (float) matchCount / keySampleSize;
		setSize(Math.round(dbsize * rate));
		ScanArgs scanArgs = ScanArgs.Builder.limit(scanCount).match(scanMatch);
		this.iterator = ScanIterator.scan((RedisKeyCommands<String, String>) commandsSupplier.apply(connection),
				scanArgs);
	}

	@Override
	protected synchronized void doClose() {
		iterator = null;
	}

	@Override
	protected synchronized String doRead() {
		if (iterator.hasNext()) {
			return iterator.next();
		}
		log.info("{} complete - {} keys read", ClassUtils.getShortName(getClass()), getCurrentItemCount());
		return null;
	}

}
