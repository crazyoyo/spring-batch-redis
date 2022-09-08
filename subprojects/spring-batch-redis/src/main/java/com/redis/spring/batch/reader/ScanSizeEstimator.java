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

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;

public class ScanSizeEstimator {

	private static final Logger log = Logger.getLogger(ScanSizeEstimator.class.getName());

	private final GenericObjectPool<StatefulConnection<String, String>> connectionPool;
	private final ScanSizeEstimatorOptions options;

	public ScanSizeEstimator(GenericObjectPool<StatefulConnection<String, String>> connectionPool,
			ScanSizeEstimatorOptions options) {
		this.connectionPool = connectionPool;
		this.options = options;
	}

	/**
	 * Estimates the number of keys that match the given pattern and type.
	 * 
	 * @return Estimated number of keys matching the given pattern and type. Returns
	 *         null if database is empty or any error occurs
	 */
	public Long execute() {
		try (StatefulConnection<String, String> connection = connectionPool.borrowObject()) {
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
		if (options.getMatch().isEmpty() && options.getType().isEmpty()) {
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
				keyTypeFutures.put(key, options.getType().isEmpty() ? null
						: ((RedisKeyAsyncCommands<String, String>) commands).type(key));
			}
			connection.flushCommands();
			Predicate<String> matchPredicate = matchPredicate();
			for (Map.Entry<String, RedisFuture<String>> entry : keyTypeFutures.entrySet()) {
				if (!matchPredicate.test(entry.getKey())) {
					continue;
				}
				Optional<String> type = options.getType();
				if (type.isEmpty() || type.get().equalsIgnoreCase(
						entry.getValue().get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS))) {
					count++;
				}
			}
			return (double) count / total;
		} finally {
			connection.setAutoFlushCommands(true);
		}
	}

	private Predicate<String> matchPredicate() {
		MatchingEngine engine = GlobPattern.compile(options.getMatch());
		return engine::matches;
	}

	public static Builder builder(GenericObjectPool<StatefulConnection<String, String>> connectionPool) {
		return new Builder(connectionPool);
	}

	public static class Builder {

		private final GenericObjectPool<StatefulConnection<String, String>> connectionPool;
		private ScanSizeEstimatorOptions options = ScanSizeEstimatorOptions.builder().build();

		public Builder(GenericObjectPool<StatefulConnection<String, String>> connectionPool) {
			this.connectionPool = connectionPool;
		}

		public Builder options(ScanSizeEstimatorOptions options) {
			Assert.notNull(options, "ScanSizeEstimatorOptions must not be null");
			this.options = options;
			return this;
		}

		public ScanSizeEstimator build() {
			return new ScanSizeEstimator(connectionPool, options);
		}

	}

}
