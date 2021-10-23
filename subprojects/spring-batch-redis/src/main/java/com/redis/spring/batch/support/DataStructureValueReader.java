package com.redis.spring.batch.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class DataStructureValueReader<K, V> extends AbstractValueReader<K, V, DataStructure<K>> {

	public DataStructureValueReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async) {
		super(connectionSupplier, poolConfig, async);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<DataStructure<K>> read(RedisModulesAsyncCommands<K, V> commands, long timeout,
			List<? extends K> keys) throws InterruptedException, ExecutionException, TimeoutException {
		List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
		for (K key : keys) {
			typeFutures.add(commands.type(key));
		}
		commands.flushCommands();
		List<DataStructure<K>> dataStructures = new ArrayList<>(keys.size());
		List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
		List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			K key = keys.get(index);
			String type = typeFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
			valueFutures.add(value(commands, key, type));
			ttlFutures.add(absoluteTTL(commands, key));
			dataStructures.add(new DataStructure<>(type, key));
		}
		commands.flushCommands();
		for (int index = 0; index < dataStructures.size(); index++) {
			DataStructure<K> dataStructure = dataStructures.get(index);
			RedisFuture<?> valueFuture = valueFutures.get(index);
			if (valueFuture != null) {
				dataStructure.setValue(valueFuture.get(timeout, TimeUnit.MILLISECONDS));
			}
			long absoluteTTL = ttlFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
			dataStructure.setAbsoluteTTL(absoluteTTL);
		}
		return dataStructures;
	}

	private RedisFuture<?> value(RedisModulesAsyncCommands<K, V> commands, K key, String type) {
		switch (type.toLowerCase()) {
		case DataStructure.HASH:
			return commands.hgetall(key);
		case DataStructure.LIST:
			return commands.lrange(key, 0, -1);
		case DataStructure.SET:
			return commands.smembers(key);
		case DataStructure.STREAM:
			return commands.xrange(key, Range.create("-", "+"));
		case DataStructure.STRING:
			return commands.get(key);
		case DataStructure.ZSET:
			return commands.zrangeWithScores(key, 0, -1);
		default:
			return null;
		}
	}

	public static DataStructureValueReaderBuilder<String, String> client(AbstractRedisClient client) {
		return new DataStructureValueReaderBuilder<>(client, StringCodec.UTF8);
	}

	public static class DataStructureValueReaderBuilder<K, V>
			extends CommandBuilder<K, V, DataStructureValueReaderBuilder<K, V>> {

		public DataStructureValueReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public DataStructureValueReader<K, V> build() {
			return new DataStructureValueReader<>(connectionSupplier(), poolConfig, async());
		}
	}

	public static class DataStructureValueReaderFactory<K, V>
			implements ValueReaderFactory<K, V, DataStructure<K>, DataStructureValueReader<K, V>> {

		@Override
		public DataStructureValueReader<K, V> create(Supplier<StatefulConnection<K, V>> connectionSupplier,
				GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
				Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async) {
			return new DataStructureValueReader<>(connectionSupplier, poolConfig, async);
		}

	}

}
