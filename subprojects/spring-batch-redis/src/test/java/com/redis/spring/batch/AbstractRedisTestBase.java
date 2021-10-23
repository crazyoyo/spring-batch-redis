package com.redis.spring.batch;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.spring.batch.support.job.JobFactory.Options;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;

@Testcontainers
@SuppressWarnings("unchecked")
public abstract class AbstractRedisTestBase extends AbstractTestBase {

	@Container
	protected static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
	@Container
	protected static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer()
			.withKeyspaceNotifications();

	static Stream<RedisServer> servers() {
		return Stream.of(REDIS, REDIS_CLUSTER);
	}

	@BeforeAll
	public static void setupRedisContainers() {
		servers().forEach(AbstractRedisTestBase::add);
	}

	protected static final Map<RedisServer, AbstractRedisClient> CLIENTS = new HashMap<>();
	protected static final Map<RedisServer, GenericObjectPool<? extends StatefulConnection<String, String>>> POOLS = new HashMap<>();
	protected static final Map<RedisServer, StatefulConnection<String, String>> CONNECTIONS = new HashMap<>();
	protected static final Map<RedisServer, StatefulRedisPubSubConnection<String, String>> PUBSUB_CONNECTIONS = new HashMap<>();
	protected static final Map<RedisServer, RedisModulesAsyncCommands<String, String>> ASYNCS = new HashMap<>();
	protected static final Map<RedisServer, RedisModulesCommands<String, String>> SYNCS = new HashMap<>();

	protected static void add(RedisServer... servers) {
		for (RedisServer server : servers) {
			if (server.isCluster()) {
				RedisModulesClusterClient client = RedisModulesClusterClient.create(server.getRedisURI());
				CLIENTS.put(server, client);
				StatefulRedisModulesClusterConnection<String, String> connection = client.connect();
				CONNECTIONS.put(server, connection);
				SYNCS.put(server, connection.sync());
				ASYNCS.put(server, connection.async());
				PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
				POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect,
						new GenericObjectPoolConfig<>()));
			} else {
				RedisModulesClient client = RedisModulesClient.create(server.getRedisURI());
				CLIENTS.put(server, client);
				StatefulRedisModulesConnection<String, String> connection = client.connect();
				CONNECTIONS.put(server, connection);
				SYNCS.put(server, connection.sync());
				ASYNCS.put(server, connection.async());
				PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
				POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect,
						new GenericObjectPoolConfig<>()));
			}
		}
	}

	@AfterEach
	public void flushall() {
		for (BaseRedisCommands<String, String> sync : SYNCS.values()) {
			((RedisServerCommands<String, String>) sync).flushall();
		}
	}

	@AfterAll
	public static void teardown() {
		for (StatefulConnection<String, String> connection : CONNECTIONS.values()) {
			connection.close();
		}
		for (StatefulRedisPubSubConnection<String, String> pubSubConnection : PUBSUB_CONNECTIONS.values()) {
			pubSubConnection.close();
		}
		for (GenericObjectPool<? extends StatefulConnection<String, String>> pool : POOLS.values()) {
			pool.close();
		}
		for (AbstractRedisClient client : CLIENTS.values()) {
			client.shutdown();
			client.getResources().shutdown();
		}
		SYNCS.clear();
		ASYNCS.clear();
		CONNECTIONS.clear();
		PUBSUB_CONNECTIONS.clear();
		POOLS.clear();
		CLIENTS.clear();
	}

	protected static <T extends AbstractRedisClient> T client(RedisServer redis) {
		return (T) CLIENTS.get(redis);
	}

	protected static RedisModulesCommands<String, String> sync(RedisServer server) {
		return SYNCS.get(server);
	}

	protected static RedisModulesAsyncCommands<String, String> async(RedisServer server) {
		return ASYNCS.get(server);
	}

	protected static <C extends StatefulConnection<String, String>> C connection(RedisServer server) {
		return (C) CONNECTIONS.get(server);
	}

	protected static <C extends StatefulRedisPubSubConnection<String, String>> C pubSubConnection(RedisServer server) {
		return (C) PUBSUB_CONNECTIONS.get(server);
	}

	protected static <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisServer server) {
		if (POOLS.containsKey(server)) {
			return (GenericObjectPool<C>) POOLS.get(server);
		}
		throw new IllegalStateException("No pool found for " + server);
	}

	protected GeneratorBuilder dataGenerator(String id, RedisServer server) {
		return Generator.id(name(server, id)).jobFactory(inMemoryJobFactory).client(client(server));
	}

	protected LiveRedisItemReader<String, KeyValue<String, byte[]>> liveKeyDumpReader(RedisServer server) {
		return RedisItemReader.keyDump(inMemoryJobFactory, client(server)).live().idleTimeout(Options.DEFAULT_IDLE_TIMEOUT)
				.build();
	}

	protected LiveRedisItemReader<String, DataStructure<String>> liveDataStructureReader(RedisServer server) {
		return RedisItemReader.dataStructure(inMemoryJobFactory, client(server)).live()
				.idleTimeout(Options.DEFAULT_IDLE_TIMEOUT).build();
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader(RedisServer server) {
		return RedisItemReader.dataStructure(inMemoryJobFactory, client(server)).build();
	}

	protected RedisItemReader<String, KeyValue<String, byte[]>> keyDumpReader(RedisServer server) {
		return RedisItemReader.keyDump(inMemoryJobFactory, client(server)).build();
	}

	protected RedisItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(RedisServer redis) {
		return RedisItemWriter.keyDump(client(redis)).build();
	}

	protected RedisItemWriter<String, String, DataStructure<String>> dataStructureWriter(RedisServer redis) {
		return RedisItemWriter.dataStructure(client(redis)).build();
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(RedisServer redis) {
		return DataStructureValueReader.client(client(redis)).build();
	}

}
