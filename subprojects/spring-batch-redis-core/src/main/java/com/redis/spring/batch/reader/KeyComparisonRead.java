package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.operation.InitializingOperation;
import com.redis.spring.batch.operation.KeyValueRead;
import com.redis.spring.batch.operation.MappingRedisFuture;
import com.redis.spring.batch.operation.OperationExecutor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonRead implements InitializingOperation<String, String, String, KeyComparison> {

	private final KeyValueRead<String, String, Object> source;
	private final KeyValueRead<String, String, Object> target;

	private OperationExecutor<String, String, String, KeyValue<String, Object>> targetOperationExecutor;
	private AbstractRedisClient targetClient;
	private int targetPoolSize;
	private ReadFrom targetReadFrom;
	private KeyComparator comparator = new KeyComparator();

	public KeyComparisonRead(KeyValueRead<String, String, Object> source, KeyValueRead<String, String, Object> target) {
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

	public void setComparator(KeyComparator comparator) {
		this.comparator = comparator;
	}

	@Override
	public void afterPropertiesSet(StatefulRedisModulesConnection<String, String> connection) throws Exception {
		Assert.notNull(targetClient, "Target Redis client not set");
		Assert.isTrue(targetPoolSize > 0, "Target pool size must be strictly positive");
		source.afterPropertiesSet(connection);
		targetOperationExecutor = new OperationExecutor<>(StringCodec.UTF8, target);
		targetOperationExecutor.setClient(targetClient);
		targetOperationExecutor.setPoolSize(targetPoolSize);
		targetOperationExecutor.setReadFrom(targetReadFrom);
		targetOperationExecutor.afterPropertiesSet();
	}

	@Override
	public void execute(BaseRedisAsyncCommands<String, String> commands, Iterable<? extends String> inputs,
			List<RedisFuture<KeyComparison>> outputs) {
		List<RedisFuture<KeyValue<String, Object>>> sourceOutputs = new ArrayList<>();
		source.execute(commands, inputs, sourceOutputs);
		Map<String, KeyValue<String, Object>> targetItems = targetOperationExecutor.apply(inputs).stream()
				.collect(Collectors.toMap(KeyValue::getKey, Function.identity()));
		Stream<RedisFuture<KeyComparison>> results = sourceOutputs.stream()
				.map(f -> new MappingRedisFuture<>(f, v -> comparator.compare(v, targetItems.get(v.getKey()))));
		results.forEach(outputs::add);
	}

}