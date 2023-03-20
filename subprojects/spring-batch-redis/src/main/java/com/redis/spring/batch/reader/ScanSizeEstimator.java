package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.util.Assert;

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

public class ScanSizeEstimator {

	private static final Logger log = Logger.getLogger(ScanSizeEstimator.class.getName());

	private final AbstractRedisClient client;
	private final ScanSizeEstimatorOptions options;

	public ScanSizeEstimator(AbstractRedisClient client, ScanSizeEstimatorOptions options) {
		this.client = client;
		this.options = options;
	}

	/**
	 * Estimates the number of keys that match the given pattern and type.
	 * 
	 * @return Estimated number of keys matching the given pattern and type. Returns
	 *         null if database is empty or any error occurs
	 */
	public Long execute() {
		try {
			StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
			return execute(connection);
		} catch (InterruptedException e) {
			log.log(Level.WARNING, "Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
			return null;
		} catch (Exception e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private Long execute(StatefulConnection<String, String> connection)
			throws InterruptedException, ExecutionException, TimeoutException {
		BaseRedisAsyncCommands<String, String> commands = Utils.async(connection);
		Long dbsize = ((RedisServerAsyncCommands<String, String>) commands).dbsize().get();
		if (dbsize == null) {
			return null;
		}
		if (options.getMatch().isEmpty() && !options.getType().isPresent()) {
			return dbsize;
		}
		double matchRate = matchRate(connection, commands);
		return Math.round(dbsize * matchRate);
	}

	@SuppressWarnings("unchecked")
	private double matchRate(StatefulConnection<String, String> connection,
			BaseRedisAsyncCommands<String, String> commands)
			throws InterruptedException, ExecutionException, TimeoutException {
		long total = options.getSampleSize();
		connection.setAutoFlushCommands(false);
		try {
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
						options.getType().isPresent() ? ((RedisKeyAsyncCommands<String, String>) commands).type(key)
								: null);
			}
			connection.flushCommands();
			Predicate<String> matchFilter = matchFilter();
			for (Map.Entry<String, RedisFuture<String>> entry : keyTypeFutures.entrySet()) {
				if (!matchFilter.test(entry.getKey())) {
					continue;
				}
				Optional<String> type = options.getType();
				if (!type.isPresent() || type.get().equalsIgnoreCase(
						entry.getValue().get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS))) {
					count++;
				}
			}
			return (double) count / total;
		} finally {
			connection.setAutoFlushCommands(true);
		}
	}

	private Predicate<String> matchFilter() {
		MatchingEngine engine = GlobPattern.compile(options.getMatch());
		return engine::matches;
	}

	public static Builder client(AbstractRedisClient client) {
		return new Builder(client);
	}

	public static class Builder {

		private final AbstractRedisClient client;
		private ScanSizeEstimatorOptions options = ScanSizeEstimatorOptions.builder().build();

		public Builder(AbstractRedisClient client) {
			this.client = client;
		}

		public Builder options(ScanSizeEstimatorOptions options) {
			Assert.notNull(options, "ScanSizeEstimatorOptions must not be null");
			this.options = options;
			return this;
		}

		public ScanSizeEstimator build() {
			return new ScanSizeEstimator(client, options);
		}

	}

}
