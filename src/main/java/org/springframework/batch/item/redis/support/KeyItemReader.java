package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyItemReader extends AbstractProgressReportingItemReader<String> {

	private final AbstractRedisClient client;
	private final KeyReaderOptions options;
	private ScanIterator<String> iterator;
	private StatefulConnection<String, String> connection;

	public KeyItemReader(AbstractRedisClient client, KeyReaderOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(client, "A Redis client is required.");
		Assert.notNull(options, "Options are required.");
		this.client = client;
		this.options = options;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected synchronized void doOpen() throws InterruptedException, ExecutionException, TimeoutException {
		if (iterator != null) {
			return;
		}
		this.connection = ClientUtils.connection(client);
		BaseRedisAsyncCommands<String, String> async = ClientUtils.async(client).apply(connection);
		async.setAutoFlushCommands(false);
		RedisFuture<Long> dbsizeFuture = ((RedisServerAsyncCommands<String, String>) async).dbsize();
		List<RedisFuture<String>> keyFutures = new ArrayList<>(options.getSampleSize());
		// rough estimate of keys matching pattern
		for (int index = 0; index < options.getSampleSize(); index++) {
			keyFutures.add(((RedisKeyAsyncCommands<String, String>) async).randomkey());
		}
		async.flushCommands();
		async.setAutoFlushCommands(true);
		long commandTimeout = client.getDefaultTimeout().getSeconds();
		int matchCount = 0;
		Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(options.getScanMatch()));
		for (RedisFuture<String> future : keyFutures) {
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
		float rate = (float) matchCount / options.getSampleSize();
		long dbsize = dbsizeFuture.get(commandTimeout, TimeUnit.SECONDS);
		setSize(Math.round(dbsize * rate));
		ScanArgs scanArgs = ScanArgs.Builder.limit(options.getScanCount()).match(options.getScanMatch());
		RedisKeyCommands<String, String> sync = (RedisKeyCommands<String, String>) ClientUtils.sync(client)
				.apply(connection);
		this.iterator = ScanIterator.scan(sync, scanArgs);
	}

	@Override
	protected synchronized void doClose() {
		connection.close();
		iterator = null;
	}

	@Override
	protected synchronized String doRead() {
		if (iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

}
