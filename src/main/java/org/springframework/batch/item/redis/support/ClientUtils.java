package org.springframework.batch.item.redis.support;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;

public class ClientUtils {

	public static <T extends StatefulConnection<?, ?>> GenericObjectPool<T> connectionPool(AbstractRedisClient client,
			GenericObjectPoolConfig<T> poolConfig) {
		return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(client), poolConfig);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T extends StatefulConnection<?, ?>> Supplier<T> connectionSupplier(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return (Supplier) ((RedisClusterClient) client)::connect;
		}
		return (Supplier) ((RedisClient) client)::connect;
	}

	@SuppressWarnings("unchecked")
	public static <T extends StatefulConnection<String, String>> Function<T, BaseRedisAsyncCommands<String, String>> async(
			AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return c -> ((StatefulRedisClusterConnection<String, String>) c).async();
		}
		return c -> ((StatefulRedisConnection<String, String>) c).async();
	}

	@SuppressWarnings("unchecked")
	public static <T extends StatefulConnection<String, String>> Function<T, BaseRedisCommands<String, String>> sync(
			AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return c -> ((StatefulRedisClusterConnection<String, String>) c).sync();
		}
		return c -> ((StatefulRedisConnection<String, String>) c).sync();
	}

	public static StatefulRedisPubSubConnection<String, String> pubSubConnection(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return ((RedisClusterClient) client).connectPubSub();
		}
		return ((RedisClient) client).connectPubSub();
	}

	public static StatefulConnection<String, String> connection(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return ((RedisClusterClient) client).connect();
		}
		return ((RedisClient) client).connect();
	}

}
