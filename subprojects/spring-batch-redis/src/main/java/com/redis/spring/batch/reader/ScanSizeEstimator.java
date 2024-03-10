package com.redis.spring.batch.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.springframework.util.StringUtils;

import com.redis.spring.batch.util.ConnectionUtils;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;

public class ScanSizeEstimator implements Callable<Long> {

	public static final long UNKNOWN_SIZE = -1;

	public static final int DEFAULT_SAMPLES = 100;

	private static final String LUA_FILE = "randomkeytype.lua";

	private final AbstractRedisClient client;

	private String scanMatch;

	private String scanType;

	private int samples = DEFAULT_SAMPLES;

	public ScanSizeEstimator(AbstractRedisClient client) {
		this.client = client;
	}

	public String getScanMatch() {
		return scanMatch;
	}

	public void setScanMatch(String match) {
		this.scanMatch = match;
	}

	public int getSamples() {
		return samples;
	}

	public void setSamples(int samples) {
		this.samples = samples;
	}

	public void setScanType(String type) {
		this.scanType = type;
	}

	public String getScanType() {
		return scanType;
	}

	/**
	 * Estimates the number of keys that match the given pattern and type.
	 * 
	 * @return Estimated number of keys matching the given pattern and type. Returns
	 *         null if database is empty or any error occurs
	 * @throws IOException if script execution exception happens during estimation
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Long call() throws Exception {
		StatefulConnection<String, String> connection = ConnectionUtils.supplier(client).get();
		BaseRedisCommands<String, String> sync = ConnectionUtils.sync(connection);
		Long dbsize = ((RedisServerCommands<String, String>) sync).dbsize();
		if (dbsize == null) {
			return UNKNOWN_SIZE;
		}
		if (!StringUtils.hasLength(scanMatch) && !StringUtils.hasLength(scanType)) {
			return dbsize;
		}
		String digest = ConnectionUtils.loadScript(client, LUA_FILE);
		RedisScriptingAsyncCommands<String, String> commands = ConnectionUtils.async(connection);
		try {
			connection.setAutoFlushCommands(false);
			List<RedisFuture<List<Object>>> futures = new ArrayList<>();
			for (int index = 0; index < samples; index++) {
				futures.add(commands.evalsha(digest, ScriptOutputType.MULTI));
			}
			connection.flushCommands();
			Predicate<String> matchPredicate = Predicates.glob(scanMatch);
			Predicate<String> typePredicate = typePredicate();
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
				if (matchPredicate.test(key) && typePredicate.test(keyType)) {
					matchCount++;
				}
			}
			double matchRate = total == 0 ? 0 : (double) matchCount / total;
			return Math.round(dbsize * matchRate);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			// Ignore and return unknown size
		} finally {
			connection.setAutoFlushCommands(true);
		}
		return UNKNOWN_SIZE;
	}

	private Predicate<String> typePredicate() {
		if (!StringUtils.hasLength(scanType)) {
			return Predicates.isTrue();
		}
		return scanType::equalsIgnoreCase;
	}

}
