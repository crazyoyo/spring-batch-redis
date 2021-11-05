package com.redis.spring.batch;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.RedisItemReader.ItemReaderBuilder;
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.builder.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

@Testcontainers
public abstract class AbstractRedisTestBase extends AbstractTestBase {

	@Container
	protected static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
	@Container
	protected static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer()
			.withKeyspaceNotifications();

	protected static final Map<RedisServer, AbstractRedisClient> clients = new HashMap<>();
	protected static final Map<RedisServer, StatefulConnection<String, String>> connections = new HashMap<>();

	static Stream<RedisServer> servers() {
		return Stream.of(REDIS, REDIS_CLUSTER);
	}

	@BeforeEach
	private void setup() {
		RedisModulesClient client = RedisModulesClient.create(REDIS.getRedisURI());
		clients.put(REDIS, client);
		connections.put(REDIS, client.connect());
		RedisModulesClusterClient clusterClient = RedisModulesClusterClient.create(REDIS_CLUSTER.getRedisURI());
		clients.put(REDIS_CLUSTER, clusterClient);
		connections.put(REDIS_CLUSTER, clusterClient.connect());
	}

	@SuppressWarnings("unchecked")
	@AfterEach
	private void teardown() {
		connections.forEach((k, v) -> {
			((RedisServerCommands<String, String>) sync(k)).flushall();
			v.close();
		});
		connections.clear();
		clients.forEach((k, v) -> {
			v.shutdown();
			v.getResources().shutdown();
		});
	}

	@SuppressWarnings("unchecked")
	protected Long dbsize(RedisServer redis) {
		return ((RedisServerCommands<String, String>) sync(redis)).dbsize();
	}

	@SuppressWarnings("unchecked")
	protected <T> T sync(RedisServer redis) {
		if (redis.isCluster()) {
			return (T) ((StatefulRedisClusterConnection<String, String>) connections.get(redis)).sync();
		}
		return (T) ((StatefulRedisConnection<String, String>) connections.get(redis)).sync();
	}

	protected GeneratorBuilder dataGenerator(RedisServer server, String name) {
		return dataGenerator(clients.get(server), name(server, name + "-generator"));
	}

	protected GeneratorBuilder dataGenerator(AbstractRedisClient client, String id) {
		if (client instanceof RedisClusterClient) {
			return configureJobRepository(Generator.builder((RedisClusterClient) client, id));
		}
		return configureJobRepository(Generator.builder((RedisClient) client, id));
	}

	protected LiveRedisItemReader<String, KeyValue<String, byte[]>> liveKeyDumpReader(RedisServer redis, String name,
			int notificationQueueCapacity) throws Exception {
		return setName(configureLiveReader(reader(redis).keyDump().live(), notificationQueueCapacity).build(), redis,
				name + "-live-key-dump");
	}

	protected ItemReaderBuilder reader(RedisServer redis) {
		AbstractRedisClient client = clients.get(redis);
		if (client instanceof RedisClusterClient) {
			return RedisItemReader.client((RedisClusterClient) client);
		}
		return RedisItemReader.client((RedisClient) client);
	}

	@SuppressWarnings("unchecked")
	private <B extends LiveRedisItemReaderBuilder<?, ?>> B configureLiveReader(B builder,
			int notificationQueueCapacity) {
		return (B) configureJobRepository(builder).idleTimeout(IDLE_TIMEOUT)
				.notificationQueueCapacity(notificationQueueCapacity);
	}

	protected LiveRedisItemReader<String, DataStructure<String>> liveDataStructureReader(RedisServer server,
			String name, int notificationQueueCapacity) throws Exception {
		return setName(configureLiveReader(reader(server).dataStructure().live(), notificationQueueCapacity).build(),
				server, name + "-live-data-structure");
	}

	private <T extends AbstractItemStreamItemReader<?>> T setName(T reader, RedisServer redis, String name) {
		reader.setName(name(redis, name + "-reader"));
		return reader;
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader(RedisServer redis, String name)
			throws Exception {
		return setName(configureJobRepository(reader(redis).dataStructure()).build(), redis, name + "-data-structure");
	}

	protected RedisItemReader<String, KeyValue<String, byte[]>> keyDumpReader(RedisServer redis, String name)
			throws Exception {
		return setName(configureJobRepository(reader(redis).keyDump()).build(), redis, name + "-key-dump");
	}

	protected RedisItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(RedisServer redis) {
		return keyDumpWriter(clients.get(redis));
	}

	protected RedisItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			RedisItemWriter.client((RedisClusterClient) client).keyDump().build();
		}
		return RedisItemWriter.client((RedisClient) client).keyDump().build();
	}

	protected RedisItemWriter<String, String, DataStructure<String>> dataStructureWriter(RedisServer redis) {
		return dataStructureWriter(clients.get(redis));
	}

	protected RedisItemWriter<String, String, DataStructure<String>> dataStructureWriter(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return RedisItemWriter.client((RedisClusterClient) client).dataStructure().build();
		}
		return RedisItemWriter.client((RedisClient) client).dataStructure().build();
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(RedisServer redis) {
		if (redis.isCluster()) {
			return DataStructureValueReader.client((RedisClusterClient) clients.get(redis)).build();
		}
		return DataStructureValueReader.client((RedisClient) clients.get(redis)).build();
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return DataStructureValueReader.client((RedisClusterClient) client).build();
		}
		return DataStructureValueReader.client((RedisClient) client).build();
	}

	protected <T> RedisItemWriterBuilder<String, String, T> redisItemWriter(RedisServer server,
			RedisOperation<String, String, T> operation) {
		if (server.isCluster()) {
			return RedisItemWriter.client((RedisClusterClient) clients.get(server)).operation(operation);
		}
		return RedisItemWriter.client((RedisClient) clients.get(server)).operation(operation);
	}

}
