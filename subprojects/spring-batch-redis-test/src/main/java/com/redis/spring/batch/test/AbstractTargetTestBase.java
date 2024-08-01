package com.redis.spring.batch.test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.ExecutionContext;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class AbstractTargetTestBase extends AbstractTestBase {

	protected RedisURI targetRedisURI;
	protected AbstractRedisClient targetRedisClient;
	protected StatefulRedisModulesConnection<String, String> targetRedisConnection;
	protected RedisModulesCommands<String, String> targetRedisCommands;

	protected abstract RedisServer getTargetRedisServer();

	@BeforeAll
	void targetSetup() {
		// Target Redis setup
		RedisServer targetRedis = getTargetRedisServer();
		if (targetRedis instanceof Startable) {
			((Startable) targetRedis).start();
		}
		targetRedisURI = redisURI(targetRedis);
		targetRedisClient = client(targetRedis);
		targetRedisConnection = RedisModulesUtils.connection(targetRedisClient);
		targetRedisCommands = targetRedisConnection.sync();
	}

	@AfterAll
	void targetTeardown() {
		if (targetRedisConnection != null) {
			targetRedisConnection.close();
			targetRedisConnection = null;
		}
		if (targetRedisClient != null) {
			targetRedisClient.shutdown();
			targetRedisClient.getResources().shutdown();
			targetRedisClient = null;
		}
		RedisServer targetRedis = getTargetRedisServer();
		if (targetRedis instanceof Startable) {
			((Startable) targetRedis).stop();
		}
	}

	@BeforeEach
	void targetFlushAll() {
		targetRedisCommands.flushall();
	}

	/**
	 * 
	 * @param info TestInfo for currently executed test case
	 * @return keyspace comparison instance
	 * @throws Exception
	 */
	protected KeyspaceComparison<String> compare(TestInfo info) throws Exception {
		assertDbNotEmpty(redisCommands);
		KeyComparisonItemReader<String, String> reader = comparisonReader(info);
		reader.open(new ExecutionContext());
		List<KeyComparison<String>> comparisons = readAll(reader);
		reader.close();
		return new KeyspaceComparison<>(comparisons);
	}

	protected void assertCompare(TestInfo info) throws Exception {
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	protected void logDiffs(Collection<KeyComparison<String>> diffs) {
		for (KeyComparison<String> diff : diffs) {
			log.error("{}: {} {}", diff.getStatus(), diff.getSource().getKey(), diff.getSource().getType());
		}
	}

	protected KeyComparisonItemReader<String, String> comparisonReader(TestInfo info) {
		return comparisonReader(info, StringCodec.UTF8);
	}

	@SuppressWarnings("unchecked")
	protected <K, V> KeyComparisonItemReader<K, V> comparisonReader(TestInfo info, RedisCodec<K, V> codec) {
		RedisItemReader<K, V, Object> sourceReader = RedisItemReader.struct(codec);
		configure(testInfo(info, "compare", "source"), sourceReader);
		RedisItemReader<K, V, Object> targetReader = RedisItemReader.struct(codec);
		targetReader.setClient(targetRedisClient);
		KeyComparisonItemReader<K, V> comparisonReader = new KeyComparisonItemReader<>(sourceReader, targetReader);
		((DefaultKeyComparator<K, V>) comparisonReader.getComparator()).setTtlTolerance(Duration.ofMillis(100));
		setName(info, comparisonReader, "compare");
		return comparisonReader;
	}

}
