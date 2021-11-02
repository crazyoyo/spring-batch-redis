package com.redis.spring.batch.support;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.redis.spring.batch.support.convert.GlobToRegexConverter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class ScanSizeEstimator implements Callable<Long> {

	public static final int DEFAULT_SAMPLE_SIZE = 1000;

	private final Supplier<StatefulConnection<String, String>> connectionSupplier;
	private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;

	private int sampleSize = DEFAULT_SAMPLE_SIZE;
	private String match;
	private String type;

	public ScanSizeEstimator(Supplier<StatefulConnection<String, String>> connectionSupplier,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async) {
		this.connectionSupplier = connectionSupplier;
		this.async = async;
	}

	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public void setType(String type) {
		this.type = type;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Long call() throws Exception {
		Utils.assertPositive(sampleSize, "Sample size");
		try (StatefulConnection<String, String> connection = connectionSupplier.get()) {
			BaseRedisAsyncCommands<String, String> commands = async.apply(connection);
			Long dbsize = ((RedisServerAsyncCommands<String, String>) commands).dbsize().get();
			if (dbsize == null) {
				return null;
			}
			if (match == null && type == null) {
				return dbsize;
			}
			commands.setAutoFlushCommands(false);
			List<RedisFuture<String>> keyFutures = new ArrayList<>(sampleSize);
			// rough estimate of keys matching pattern
			for (int index = 0; index < sampleSize; index++) {
				keyFutures.add(((RedisKeyAsyncCommands<String, String>) commands).randomkey());
			}
			commands.flushCommands();
			long commandTimeout = connection.getTimeout().toMillis();
			int matchCount = 0;
			Map<String, RedisFuture<String>> keyTypeFutures = new HashMap<>();
			for (RedisFuture<String> future : keyFutures) {
				String key = future.get(commandTimeout, TimeUnit.MILLISECONDS);
				if (key == null) {
					continue;
				}
				keyTypeFutures.put(key,
						type == null ? null : ((RedisKeyAsyncCommands<String, String>) commands).type(key));
			}
			commands.flushCommands();
			Predicate<String> matchPredicate = predicate(match);
			for (Map.Entry<String, RedisFuture<String>> entry : keyTypeFutures.entrySet()) {
				if (!matchPredicate.test(entry.getKey())) {
					continue;
				}
				if (type == null
						|| type.equalsIgnoreCase(entry.getValue().get(commandTimeout, TimeUnit.MILLISECONDS))) {
					matchCount++;
				}
			}
			commands.setAutoFlushCommands(true);
			return dbsize * matchCount / sampleSize;
		}

	}

	private Predicate<String> predicate(String match) {
		if (match == null) {
			return k -> true;
		}
		Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(match));
		return k -> pattern.matcher(k).matches();
	}

	public static ScanSizeEstimatorBuilder client(AbstractRedisClient client) {
		return new ScanSizeEstimatorBuilder(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class ScanSizeEstimatorBuilder extends CommandBuilder<String, String, ScanSizeEstimatorBuilder> {

		private int sampleSize = DEFAULT_SAMPLE_SIZE;
		private String match;
		private String type;

		public ScanSizeEstimatorBuilder(AbstractRedisClient client) {
			super(client, StringCodec.UTF8);
		}

		public ScanSizeEstimator build() {
			ScanSizeEstimator estimator = new ScanSizeEstimator(super.connectionSupplier(), super.async());
			estimator.setSampleSize(sampleSize);
			estimator.setMatch(match);
			estimator.setType(type);
			return estimator;
		}

	}

}
