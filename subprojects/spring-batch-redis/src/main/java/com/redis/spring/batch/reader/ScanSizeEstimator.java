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
import java.util.logging.Level;
import java.util.logging.Logger;

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

	public static final long DEFAULT_SAMPLE_SIZE = 100;
	public static final long UNKNOWN_SIZE = -1;

	private static final Logger log = Logger.getLogger(ScanSizeEstimator.class.getName());

	private final AbstractRedisClient client;
	private final String match;
	private final long sampleSize;
	private final Optional<String> type;

	public ScanSizeEstimator(Builder builder) {
		this.client = builder.client;
		this.match = builder.match;
		this.sampleSize = builder.sampleSize;
		this.type = builder.type;
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
			log.log(Level.WARNING, "Interrupted!", e);
			// Restore interrupted state...
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

	public static Builder client(AbstractRedisClient client) {
		return new Builder(client);
	}

	public static class Builder {

		private final AbstractRedisClient client;
		private String match = ScanOptions.DEFAULT_MATCH;
		private long sampleSize = DEFAULT_SAMPLE_SIZE;
		private Optional<String> type = Optional.empty();

		public Builder(AbstractRedisClient client) {
			this.client = client;
		}

		public Builder match(String match) {
			this.match = match;
			return this;
		}

		public Builder sampleSize(long sampleSize) {
			this.sampleSize = sampleSize;
			return this;
		}

		public Builder type(String type) {
			return type(Optional.of(type));
		}

		public Builder type(Optional<String> type) {
			this.type = type;
			return this;
		}

		public ScanSizeEstimator build() {
			return new ScanSizeEstimator(this);
		}

	}

}
