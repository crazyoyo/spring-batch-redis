package com.redis.spring.batch.reader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class KeyValueReadOperation<K, V> implements Operation<K, V, K, KeyValue<K>> {

	public enum Type {
		DUMP, STRUCT
	}

	private static final String FILENAME = "keyvalue.lua";

	private final Evalsha<K, V, K> evalsha;

	private final Function<V, String> toStringValueFunction;

	private UnaryOperator<KeyValue<K>> postOperator = UnaryOperator.identity();

	public KeyValueReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec, DataSize memLimit, int samples,
			Type type) throws IOException {
		this.evalsha = new Evalsha<>(FILENAME, client, codec);
		this.evalsha.setKeyFunction(Function.identity());
		this.evalsha.setArgs(memLimit.toBytes(), samples, type.name().toLowerCase());
		this.toStringValueFunction = CodecUtils.toStringValueFunction(codec);
	}

	public void setPostOperator(UnaryOperator<KeyValue<K>> operator) {
		this.postOperator = operator;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends K> inputs,
			Chunk<RedisFuture<KeyValue<K>>> outputs) {
		Chunk<RedisFuture<List<Object>>> evalshaOutputs = new Chunk<>();
		evalsha.execute(commands, inputs, evalshaOutputs);
		for (RedisFuture<List<Object>> future : evalshaOutputs) {
			outputs.add(new MappingRedisFuture<>(future.toCompletableFuture(), this::toKeyValue));
		}
	}

	@SuppressWarnings("unchecked")
	protected KeyValue<K> toKeyValue(List<Object> list) {
		if (CollectionUtils.isEmpty(list)) {
			return null;
		}
		Iterator<Object> iterator = list.iterator();
		if (!iterator.hasNext()) {
			return null;
		}
		KeyValue<K> keyValue = new KeyValue<>();
		keyValue.setKey((K) iterator.next());
		if (iterator.hasNext()) {
			keyValue.setTtl((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setMemoryUsage((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setType(DataType.of(toString(iterator.next())));
		}
		if (iterator.hasNext()) {
			keyValue.setValue(iterator.next());
		}
		return postOperator.apply(keyValue);
	}

	@SuppressWarnings("unchecked")
	protected String toString(Object value) {
		return toStringValueFunction.apply((V) value);
	}

}
