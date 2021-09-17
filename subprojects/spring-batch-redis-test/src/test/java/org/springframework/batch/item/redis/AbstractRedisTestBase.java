package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;
import org.springframework.batch.item.redis.test.DataGenerator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Testcontainers
@SuppressWarnings({"unchecked", "unused", "SingleStatementInBlock", "SameParameterValue"})
public abstract class AbstractRedisTestBase extends AbstractTestBase {

    @Container
    protected static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();

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
    protected static final Map<RedisServer, BaseRedisAsyncCommands<String, String>> ASYNCS = new HashMap<>();
    protected static final Map<RedisServer, BaseRedisCommands<String, String>> SYNCS = new HashMap<>();

    protected static void add(RedisServer... servers) {
        for (RedisServer server : servers) {
            if (server.isCluster()) {
                RedisModulesClusterClient client = RedisModulesClusterClient.create(server.getRedisURI());
                CLIENTS.put(server, client);
                StatefulRedisClusterConnection<String, String> connection = client.connect();
                CONNECTIONS.put(server, connection);
                SYNCS.put(server, connection.sync());
                ASYNCS.put(server, connection.async());
                PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
                POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
            } else {
                RedisModulesClient client = RedisModulesClient.create(server.getRedisURI());
                CLIENTS.put(server, client);
                StatefulRedisConnection<String, String> connection = client.connect();
                CONNECTIONS.put(server, connection);
                SYNCS.put(server, connection.sync());
                ASYNCS.put(server, connection.async());
                PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
                POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
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

    protected static RedisModulesClient redisClient(RedisServer redis) {
        return client(redis);
    }

    protected static RedisModulesClusterClient redisClusterClient(RedisServer redis) {
        return client(redis);
    }

    protected static <T> T sync(RedisServer server) {
        return (T) SYNCS.get(server);
    }

    protected static <T> T async(RedisServer server) {
        return (T) ASYNCS.get(server);
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

    protected String name(RedisServer server, String name) {
        if (server.isCluster()) {
            return "cluster-" + name;
        }
        return name;
    }

    protected DataGenerator.DataGeneratorBuilder dataGenerator(RedisServer server) {
        if (server.isCluster()) {
            return DataGenerator.client(redisClusterClient(server));
        }
        return DataGenerator.client(redisClient(server));
    }

    protected LiveKeyValueItemReader<KeyValue<byte[]>> liveKeyDumpReader(RedisServer server) {
        Duration idleTimeout = Duration.ofMillis(500);
        if (server.isCluster()) {
            return KeyDumpItemReader.client(redisClusterClient(server)).live().idleTimeout(idleTimeout).build();
        }
        return KeyDumpItemReader.client(redisClient(server)).live().idleTimeout(idleTimeout).build();
    }

    protected KeyValueItemReader<KeyValue<byte[]>> keyDumpReader(RedisServer server) {
        if (server.isCluster()) {
            return KeyDumpItemReader.client(redisClusterClient(server)).build();
        }
        return KeyDumpItemReader.client(redisClient(server)).build();
    }


    protected KeyDumpItemWriter keyDumpWriter(RedisServer redis) {
        if (redis.isCluster()) {
            return KeyDumpItemWriter.client(redisClusterClient(redis)).build();
        }
        return KeyDumpItemWriter.client(redisClient(redis)).build();
    }

    protected KeyValueItemReader<DataStructure> dataStructureReader(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureItemReader.client(redisClusterClient(server)).build();
        }
        return DataStructureItemReader.client(redisClient(server)).build();
    }

    protected DataStructureValueReader dataStructureValueReader(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureValueReader.client(redisClusterClient(server)).build();
        }
        return DataStructureValueReader.client(redisClient(server)).build();
    }

    protected DataStructureItemWriter<DataStructure> dataStructureWriter(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureItemWriter.client(redisClusterClient(server)).build();
        }
        return DataStructureItemWriter.client(redisClient(server)).build();
    }

}
