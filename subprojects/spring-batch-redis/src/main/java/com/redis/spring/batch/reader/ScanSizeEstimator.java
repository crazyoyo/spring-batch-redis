package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;

public class ScanSizeEstimator implements LongSupplier {

	private static final Log log = LogFactory.getLog(ScanSizeEstimator.class);

	public static final long UNKNOWN_SIZE = -1;
	public static final long DEFAULT_SAMPLE_SIZE = 100;
	private static final String FILENAME = "randomkeytype.lua";

	private final Supplier<StatefulConnection<String, String>> connectionSupplier;
	private ScanOptions options = ScanOptions.builder().build();

	public ScanSizeEstimator(Supplier<StatefulConnection<String, String>> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	public ScanOptions getOptions() {
		return options;
	}

	public void setOptions(ScanOptions options) {
		this.options = options;
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
		StatefulConnection<String, String> connection = connectionSupplier.get();
		BaseRedisCommands<String, String> sync = Utils.sync(connection);
		Long dbsize = ((RedisServerCommands<String, String>) sync).dbsize();
		if (dbsize == null) {
			return UNKNOWN_SIZE;
		}
		if (ScanOptions.MATCH_ALL.equals(options.getMatch()) && !options.getType().isPresent()) {
			return dbsize;
		}
		String digest = Utils.loadScript(connectionSupplier, FILENAME);
		RedisScriptingAsyncCommands<String, String> commands = Utils.async(connection);
		try {
			connection.setAutoFlushCommands(false);
			List<RedisFuture<List<Object>>> futures = new ArrayList<>();
			for (int index = 0; index < options.getCount(); index++) {
				futures.add(commands.evalsha(digest, ScriptOutputType.MULTI));
			}
			connection.flushCommands();
			MatchingEngine matchingEngine = GlobPattern.compile(options.getMatch());
			Predicate<String> typePredicate = options.getType().map(t -> caseInsensitivePredicate(t)).orElse(s -> true);
			int total = 0;
			int matchCount = 0;
			for (RedisFuture<List<Object>> future : futures) {
				List<Object> result = future.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
				if (result.size() != 2) {
					continue;
				}
				String key = (String) result.get(0);
				String keyType = (String) result.get(1);
				total++;
				if (matchingEngine.matches(key) && typePredicate.test(keyType)) {
					matchCount++;
				}
			}
			double matchRate = total == 0 ? 0 : (double) matchCount / total;
			return Math.round(dbsize * matchRate);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			log.error("Could not estimate size", e);
		} finally {
			connection.setAutoFlushCommands(true);
		}
		return UNKNOWN_SIZE;
	}

	private static Predicate<String> caseInsensitivePredicate(String expected) {
		return expected::equalsIgnoreCase;
	}

}
