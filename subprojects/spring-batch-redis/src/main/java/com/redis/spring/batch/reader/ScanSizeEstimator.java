package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;

public class ScanSizeEstimator implements LongSupplier {

	public static final long UNKNOWN_SIZE = -1;
	public static final long DEFAULT_SAMPLE_SIZE = 100;
	public static final String DEFAULT_MATCH = ScanKeyItemReader.DEFAULT_MATCH;

	private final AbstractRedisClient client;

	private String match = DEFAULT_MATCH;
	private long sampleSize = DEFAULT_SAMPLE_SIZE;
	private Optional<String> type = Optional.empty();

	public ScanSizeEstimator(AbstractRedisClient client) {
		this.client = client;
	}

	public ScanSizeEstimator withMatch(String match) {
		this.match = match;
		return this;
	}

	public ScanSizeEstimator withSampleSize(long sampleSize) {
		this.sampleSize = sampleSize;
		return this;
	}

	public ScanSizeEstimator withType(String type) {
		return withType(Optional.of(type));
	}

	public ScanSizeEstimator withType(Optional<String> type) {
		this.type = type;
		return this;
	}

	/**
	 * Estimates the number of keys that match the given pattern and type.
	 * 
	 * @return Estimated number of keys matching the given pattern and type. Returns
	 *         null if database is empty or any error occurs
	 */
	@Override
	@SuppressWarnings("unchecked")
	public long getAsLong() {
		StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
		BaseRedisAsyncCommands<String, String> commands = Utils.async(connection);
		try {
			Long dbsize = ((RedisServerAsyncCommands<String, String>) commands).dbsize().get();
			if (dbsize == null) {
				return UNKNOWN_SIZE;
			}
			if (match.isEmpty() && !type.isPresent()) {
				return dbsize;
			}
			connection.setAutoFlushCommands(false);
			try {
				double matchRate = matchRate(connection, commands);
				return Math.round(dbsize * matchRate);
			} finally {
				connection.setAutoFlushCommands(true);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			// ignore
		}
		return UNKNOWN_SIZE;
	}

	@SuppressWarnings("unchecked")
	private double matchRate(StatefulConnection<String, String> connection,
			BaseRedisAsyncCommands<String, String> commands)
			throws InterruptedException, ExecutionException, TimeoutException {
		long total = sampleSize;
		List<RedisFuture<String>> keyFutures = new ArrayList<>();
		// rough estimate of keys matching pattern
		for (int index = 0; index < total; index++) {
			keyFutures.add(((RedisKeyAsyncCommands<String, String>) commands).randomkey());
		}
		connection.flushCommands();
		int count = 0;
		Map<String, RedisFuture<String>> keyTypeFutures = new HashMap<>();
		for (RedisFuture<String> future : keyFutures) {
			String key = future.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			if (key == null) {
				continue;
			}
			keyTypeFutures.put(key,
					type.isPresent() ? ((RedisKeyAsyncCommands<String, String>) commands).type(key) : null);
		}
		connection.flushCommands();
		Predicate<String> matchFilter = matchFilter();
		for (Map.Entry<String, RedisFuture<String>> entry : keyTypeFutures.entrySet()) {
			if (!matchFilter.test(entry.getKey())) {
				continue;
			}
			if (!type.isPresent() || type.get().equalsIgnoreCase(
					entry.getValue().get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS))) {
				count++;
			}
		}
		return (double) count / total;
	}

	private Predicate<String> matchFilter() {
		MatchingEngine engine = GlobPattern.compile(match);
		return engine::matches;
	}

}
