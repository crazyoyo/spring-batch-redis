package com.redis.spring.batch.support;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.support.convert.GlobToRegexConverter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.StringCodec;
import lombok.Builder;
import lombok.Data;

public class ScanSizeEstimator {

	private final Supplier<StatefulConnection<String, String>> connectionSupplier;
	private final Function<StatefulConnection<String, String>, RedisModulesAsyncCommands<String, String>> async;

	public ScanSizeEstimator(Supplier<StatefulConnection<String, String>> connectionSupplier,
			Function<StatefulConnection<String, String>, RedisModulesAsyncCommands<String, String>> async) {
		this.connectionSupplier = connectionSupplier;
		this.async = async;
	}

	public long estimate(EstimateOptions options) throws Exception {
		Utils.assertPositive(options.getSampleSize(), "Sample size");
		try (StatefulConnection<String, String> connection = connectionSupplier.get()) {
			RedisModulesAsyncCommands<String, String> commands = async.apply(connection);
			Long dbsize = commands.dbsize().get();
			if (dbsize == null) {
				throw new Exception("Could not get DB size");
			}
			if (options.getMatch() == null && options.getType() == null) {
				return dbsize;
			}
			commands.setAutoFlushCommands(false);
			List<RedisFuture<String>> keyFutures = new ArrayList<>(options.getSampleSize());
			// rough estimate of keys matching pattern
			for (int index = 0; index < options.getSampleSize(); index++) {
				keyFutures.add(commands.randomkey());
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
				keyTypeFutures.put(key, options.getType() == null ? null : commands.type(key));
			}
			commands.flushCommands();
			Predicate<String> matchPredicate = predicate(options.getMatch());
			for (Map.Entry<String, RedisFuture<String>> entry : keyTypeFutures.entrySet()) {
				if (matchPredicate.test(entry.getKey())) {
					if (options.getType() == null || options.getType()
							.equalsIgnoreCase(entry.getValue().get(commandTimeout, TimeUnit.MILLISECONDS))) {
						matchCount++;
					}
				}
			}
			commands.setAutoFlushCommands(true);
			return dbsize * matchCount / options.getSampleSize();
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

	public static class ScanSizeEstimatorBuilder extends CommandBuilder<String, String, ScanSizeEstimatorBuilder> {

		public ScanSizeEstimatorBuilder(AbstractRedisClient client) {
			super(client, StringCodec.UTF8);
		}

		public ScanSizeEstimator build() {
			return new ScanSizeEstimator(connectionSupplier(), async());
		}

	}

	@Data
	@Builder
	public static class EstimateOptions {

		public final static int DEFAULT_SAMPLE_SIZE = 1000;

		@Builder.Default
		private int sampleSize = DEFAULT_SAMPLE_SIZE;
		private String match;
		private String type;

		public EstimateOptions sampleSize(int sampleSize) {
			Utils.assertPositive(sampleSize, "Sample size");
			this.sampleSize = sampleSize;
			return this;
		}

	}

}
