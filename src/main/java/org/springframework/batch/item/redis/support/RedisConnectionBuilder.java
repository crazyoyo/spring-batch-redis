package org.springframework.batch.item.redis.support;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

@SuppressWarnings("unchecked")
public class RedisConnectionBuilder<K, V, B extends RedisConnectionBuilder<K, V, B>> {

	private final RedisCodec<K, V> codec;
	private RedisURI uri;
	private ClientResources clientResources;
	private ClusterClientOptions clientOptions;
	private GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig = new GenericObjectPoolConfig<>();
	private boolean cluster;
	
	public RedisConnectionBuilder(RedisCodec<K, V> codec) {
		this.codec = codec;
	}

	public RedisURI uri() {
		return uri;
	}

	public B uri(RedisURI uri) {
		this.uri = uri;
		return (B) this;
	}

	public B clientResources(ClientResources clientResources) {
		this.clientResources = clientResources;
		return (B) this;
	}

	public B poolConfig(GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig) {
		this.poolConfig = poolConfig;
		return (B) this;
	}

	public B clientOptions(ClusterClientOptions clientOptions) {
		this.clientOptions = clientOptions;
		return (B) this;
	}

	public B cluster(boolean cluster) {
		this.cluster = cluster;
		return (B) this;
	}

	public Supplier<StatefulConnection<K, V>> connectionSupplier() {
		if (cluster) {
			RedisClusterClient client = clusterClient();
			return () -> client.connect(codec);
		}
		RedisClient client = client();
		return () -> client.connect(codec);
	}

	public StatefulConnection<K, V> connection() {
		if (cluster) {
			return clusterClient().connect(codec);
		}
		return client().connect(codec);
	}

	public StatefulRedisPubSubConnection<K, V> pubSubConnection() {
		if (cluster) {
			return clusterClient().connectPubSub(codec);
		}
		return client().connectPubSub(codec);
	}

	public RedisClient client() {
		RedisClient client = client(uri, clientResources);
		if (clientOptions != null) {
			client.setOptions(clientOptions);
		}
		return client;
	}

	public RedisClusterClient clusterClient() {
		RedisClusterClient client = clusterClient(uri, clientResources);
		if (clientOptions != null) {
			client.setOptions(clientOptions);
		}
		return client;
	}

	private RedisClient client(RedisURI redisURI, ClientResources clientResources) {
		if (clientResources == null) {
			return RedisClient.create(redisURI);
		}
		return RedisClient.create(clientResources, redisURI);
	}

	private RedisClusterClient clusterClient(RedisURI redisURI, ClientResources clientResources) {
		if (clientResources == null) {
			return RedisClusterClient.create(redisURI);
		}
		return RedisClusterClient.create(clientResources, redisURI);
	}

	public Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync() {
		if (cluster) {
			return c -> ((StatefulRedisClusterConnection<K, V>) c).sync();
		}
		return c -> ((StatefulRedisConnection<K, V>) c).sync();
	}

	public Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async() {
		if (cluster) {
			return c -> ((StatefulRedisClusterConnection<K, V>) c).async();
		}
		return c -> ((StatefulRedisConnection<K, V>) c).async();
	}

	public GenericObjectPool<StatefulConnection<K, V>> pool() {
		return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(), poolConfig);
	}
	
	protected Duration timeout() {
		return uri().getTimeout();
	}

}
