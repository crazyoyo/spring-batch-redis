package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import com.redis.spring.batch.support.RedisConnectionBuilder;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import io.lettuce.core.codec.StringCodec;

public class RedisScanSizeEstimator {

	private final Logger log = Logger.getLogger(getClass().getName());

	public static final int DEFAULT_SAMPLE_SIZE = 1000;

	private final Supplier<StatefulConnection<String, String>> connectionSupplier;
	private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;

	private int sampleSize = DEFAULT_SAMPLE_SIZE;
	private Optional<String> match = Optional.empty();
	private Optional<String> type = Optional.empty();

	public RedisScanSizeEstimator(Supplier<StatefulConnection<String, String>> connectionSupplier,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async) {
		this.connectionSupplier = connectionSupplier;
		this.async = async;
	}

	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}

	public void setMatch(Optional<String> match) {
		this.match = match;
	}

	public void setType(Optional<String> type) {
		this.type = type;
	}

	/**
	 * Estimates the number of keys that match the given pattern and type.
	 * 
	 * @return Estimated number of keys matching the given pattern and type. Returns
	 *         null if database is empty or any error occurs
	 */
	public Long execute() {
		Utils.assertPositive(sampleSize, "Sample size");
		try (StatefulConnection<String, String> connection = connectionSupplier.get()) {
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
		BaseRedisAsyncCommands<String, String> commands = async.apply(connection);
		Long dbsize = ((RedisServerAsyncCommands<String, String>) commands).dbsize().get();
		if (dbsize == null) {
			return null;
		}
		if (match.isEmpty() && type.isEmpty()) {
			return dbsize;
		}
		commands.setAutoFlushCommands(false);
		long commandTimeout = connection.getTimeout().toMillis();
		List<RedisFuture<String>> keyFutures = new ArrayList<>(sampleSize);
		// rough estimate of keys matching pattern
		for (int index = 0; index < sampleSize; index++) {
			keyFutures.add(((RedisKeyAsyncCommands<String, String>) commands).randomkey());
		}
		commands.flushCommands();
		int matchCount = 0;
		Map<String, RedisFuture<String>> keyTypeFutures = new HashMap<>();
		for (RedisFuture<String> future : keyFutures) {
			String key = future.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			if (key == null) {
				continue;
			}
			keyTypeFutures.put(key,
					type.isEmpty() ? null : ((RedisKeyAsyncCommands<String, String>) commands).type(key));
		}
		commands.flushCommands();
		Predicate<String> matchPredicate = matchPredicate();
		for (Map.Entry<String, RedisFuture<String>> entry : keyTypeFutures.entrySet()) {
			if (!matchPredicate.test(entry.getKey())) {
				continue;
			}
			if (type.isEmpty()
					|| type.get().equalsIgnoreCase(entry.getValue().get(commandTimeout, TimeUnit.MILLISECONDS))) {
				matchCount++;
			}
		}
		commands.setAutoFlushCommands(true);
		return dbsize * matchCount / sampleSize;
	}

	private Predicate<String> matchPredicate() {
		if (match.isEmpty()) {
			return k -> true;
		}
		MatchingEngine engine = GlobPattern.compile(match.get());
		return engine::matches;
	}

	public static Builder client(AbstractRedisClient client) {
		return new Builder(client);
	}

	public static class Builder extends RedisConnectionBuilder<String, String, Builder> {

		private int sampleSize = DEFAULT_SAMPLE_SIZE;
		private Optional<String> match = Optional.empty();
		private Optional<String> type = Optional.empty();

		public Builder(AbstractRedisClient client) {
			super(client, StringCodec.UTF8);
		}

		public Builder sampleSize(int sampleSize) {
			this.sampleSize = sampleSize;
			return this;
		}

		public Builder match(String match) {
			this.match = Optional.of(match);
			return this;
		}

		public Builder type(String type) {
			this.type = Optional.of(type);
			return this;
		}

		public Builder type(Optional<String> type) {
			this.type = type;
			return this;
		}

		public RedisScanSizeEstimator build() {
			RedisScanSizeEstimator estimator = new RedisScanSizeEstimator(super.connectionSupplier(), super.async());
			estimator.setSampleSize(sampleSize);
			estimator.setMatch(match);
			estimator.setType(type);
			return estimator;
		}

	}

}
