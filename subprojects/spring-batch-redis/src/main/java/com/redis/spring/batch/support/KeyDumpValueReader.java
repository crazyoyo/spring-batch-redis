package com.redis.spring.batch.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyDumpValueReader<K, V> extends AbstractValueReader<K, V, KeyValue<K, byte[]>> {

	public KeyDumpValueReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async) {
		super(connectionSupplier, poolConfig, async);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<KeyValue<K, byte[]>> read(RedisModulesAsyncCommands<K, V> commands, long timeout,
			List<? extends K> keys) throws InterruptedException, ExecutionException, TimeoutException {
		List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
		List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
		for (K key : keys) {
			ttlFutures.add(absoluteTTL(commands, key));
			dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
		}
		commands.flushCommands();
		List<KeyValue<K, byte[]>> dumps = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			K key = keys.get(index);
			long absoluteTTL = ttlFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
			byte[] bytes = dumpFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
			dumps.add(new KeyValue<>(key, absoluteTTL, bytes));
		}
		return dumps;
	}

	public static KeyDumpValueReaderBuilder<String, String> client(RedisModulesClient client) {
		return new KeyDumpValueReaderBuilder<>(client, StringCodec.UTF8);
	}

	public static KeyDumpValueReaderBuilder<String, String> client(RedisModulesClusterClient client) {
		return new KeyDumpValueReaderBuilder<>(client, StringCodec.UTF8);
	}

	public static class KeyDumpValueReaderBuilder<K, V> extends CommandBuilder<K, V, KeyDumpValueReaderBuilder<K, V>> {

		public KeyDumpValueReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public KeyDumpValueReader<K, V> build() {
			return new KeyDumpValueReader<>(connectionSupplier(), poolConfig, async());
		}
	}

	public static class KeyDumpValueReaderFactory<K, V>
			implements ValueReaderFactory<K, V, KeyValue<K, byte[]>, KeyDumpValueReader<K, V>> {

		@Override
		public KeyDumpValueReader<K, V> create(Supplier<StatefulConnection<K, V>> connectionSupplier,
				GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
				Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async) {
			return new KeyDumpValueReader<>(connectionSupplier, poolConfig, async);
		}

	}

}
