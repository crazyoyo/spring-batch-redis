package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

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
	private String match = ScanKeyItemReader.DEFAULT_MATCH;
	private long sampleSize = DEFAULT_SAMPLE_SIZE;
	private Optional<String> type = Optional.empty();

	public ScanSizeEstimator(Supplier<StatefulConnection<String, String>> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	public void setMatch(String match) {
		Assert.notNull(match, "Match must be null");
		Assert.isTrue(!match.trim().isEmpty(), "Match must not be empty");
		this.match = match;
	}

	public void setSampleSize(long sampleSize) {
		this.sampleSize = sampleSize;
	}

	public void setType(String type) {
		setType(Optional.of(type));
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
	@Override
	@SuppressWarnings("unchecked")
	public long getAsLong() {
		StatefulConnection<String, String> connection = connectionSupplier.get();
		BaseRedisCommands<String, String> sync = Utils.sync(connection);
		Long dbsize = ((RedisServerCommands<String, String>) sync).dbsize();
		if (dbsize == null) {
			return UNKNOWN_SIZE;
		}
		if (ScanKeyItemReader.MATCH_ALL.equals(match) && !type.isPresent()) {
			return dbsize;
		}
		String digest = Utils.loadScript(connectionSupplier, FILENAME);
		RedisScriptingAsyncCommands<String, String> commands = Utils.async(connection);
		try {
			connection.setAutoFlushCommands(false);
			List<RedisFuture<List<Object>>> futures = new ArrayList<>();
			for (int index = 0; index < sampleSize; index++) {
				futures.add(commands.evalsha(digest, ScriptOutputType.MULTI));
			}
			connection.flushCommands();
			MatchingEngine matchingEngine = GlobPattern.compile(match);
			Predicate<String> typePredicate = type.map(t -> caseInsensitivePredicate(t)).orElse(s -> true);
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
