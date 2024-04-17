package com.redis.spring.batch.test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public abstract class AbstractTargetTestBase extends AbstractTestBase {

	private final Logger log = LoggerFactory.getLogger(AbstractTargetTestBase.class);

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
		}
		if (targetRedisClient != null) {
			targetRedisClient.shutdown();
			targetRedisClient.getResources().shutdown();
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
	protected KeyspaceComparison compare(TestInfo info) throws Exception {
		assertDbNotEmpty(redisCommands);
		KeyComparisonItemReader reader = comparisonReader(testInfo(info, "compare"));
		reader.open(new ExecutionContext());
		List<KeyComparison> comparisons = readAll(reader);
		reader.close();
		Assertions.assertFalse(comparisons.isEmpty());
		return new KeyspaceComparison(comparisons);
	}

	protected void logDiffs(Collection<KeyComparison> diffs) {
		for (KeyComparison diff : diffs) {
			log.error("{}: {} {}", diff.getStatus(), diff.getSource().getKey(), diff.getSource().getType());
		}
	}

	protected KeyComparisonItemReader comparisonReader(TestInfo info) {
		KeyComparisonItemReader reader = RedisItemReader.compare();
		configure(info, reader, "comparison");
		reader.setTtlTolerance(Duration.ofMillis(100));
		reader.setClient(redisClient);
		reader.setTargetClient(targetRedisClient);
		return reader;
	}

}
