package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.OperationExecutor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class KeyComparisonRead<K, V> implements InitializingOperation<K, V, K, KeyComparison<K>> {

	private final MemKeyValueRead<K, V, Object> source;
	private final MemKeyValueRead<K, V, Object> target;

	private OperationExecutor<K, V, K, MemKeyValue<K, Object>> targetOperationExecutor;
	private AbstractRedisClient targetClient;
	private int targetPoolSize;
	private ReadFrom targetReadFrom;
	private KeyComparator<K, V> comparator = new KeyComparator<>();
	private RedisCodec<K, V> codec;

	public KeyComparisonRead(RedisCodec<K, V> codec, MemKeyValueRead<K, V, Object> source,
			MemKeyValueRead<K, V, Object> target) {
		this.codec = codec;
		this.source = source;
		this.target = target;
	}

	public void setTargetClient(AbstractRedisClient targetClient) {
		this.targetClient = targetClient;
	}

	public void setTargetPoolSize(int targetPoolSize) {
		this.targetPoolSize = targetPoolSize;
	}

	public void setTargetReadFrom(ReadFrom targetReadFrom) {
		this.targetReadFrom = targetReadFrom;
	}

	public void setComparator(KeyComparator<K, V> comparator) {
		this.comparator = comparator;
	}

	@Override
	public void afterPropertiesSet(StatefulRedisModulesConnection<K, V> connection) throws Exception {
		Assert.notNull(targetClient, "Target Redis client not set");
		Assert.isTrue(targetPoolSize > 0, "Target pool size must be strictly positive");
		source.afterPropertiesSet(connection);
		targetOperationExecutor = new OperationExecutor<>(codec, target);
		targetOperationExecutor.setClient(targetClient);
		targetOperationExecutor.setPoolSize(targetPoolSize);
		targetOperationExecutor.setReadFrom(targetReadFrom);
		targetOperationExecutor.afterPropertiesSet();
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends K> inputs,
			List<RedisFuture<KeyComparison<K>>> outputs) {
		List<RedisFuture<MemKeyValue<K, Object>>> sourceOutputs = new ArrayList<>();
		source.execute(commands, inputs, sourceOutputs);
		List<MemKeyValue<K, Object>> targetItems = targetOperationExecutor.apply(inputs);
		for (int index = 0; index < sourceOutputs.size(); index++) {
			RedisFuture<MemKeyValue<K, Object>> sourceOutput = sourceOutputs.get(index);
			KeyValue<K, Object> targetItem = targetItems.get(index);
			outputs.add(new MappingRedisFuture<>(sourceOutput, v -> comparator.compare(v, targetItem)));
		}
	}

}