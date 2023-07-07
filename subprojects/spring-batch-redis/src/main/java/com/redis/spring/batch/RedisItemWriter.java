package com.redis.spring.batch;

import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.common.ValueType;
import com.redis.spring.batch.writer.AbstractRedisItemWriter;
import com.redis.spring.batch.writer.StructOptions;
import com.redis.spring.batch.writer.StructWriteOperation;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.RestoreReplace;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V> extends AbstractRedisItemWriter<K, V, KeyValue<K>> {

	private final ValueType valueType;
	private StructOptions structWriteOptions = StructOptions.builder().build();

	public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, ValueType valueType) {
		super(client, codec);
		this.valueType = valueType;
	}

	public StructOptions getStructWriteOptions() {
		return structWriteOptions;
	}

	public void setStructOptions(StructOptions structWriteOptions) {
		this.structWriteOptions = structWriteOptions;
	}

	public ValueType getValueType() {
		return valueType;
	}

	@Override
	protected BatchOperation<K, V, KeyValue<K>, Object> operation() {
		if (valueType == ValueType.DUMP) {
			return new SimpleBatchOperation<>(restoreOperation());
		}
		StructWriteOperation<K, V> operation = new StructWriteOperation<>();
		operation.setOptions(structWriteOptions);
		return operation;
	}

	private RestoreReplace<K, V, KeyValue<K>> restoreOperation() {
		return new RestoreReplace<>(KeyValue::getKey, v -> (byte[]) v.getValue(), KeyValue::getTtl);
	}

	public static Builder<String, String> client(AbstractRedisClient client) {
		return client(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> client(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public abstract static class BaseBuilder<K, V, B extends BaseBuilder<K, V, B>> {

		protected final AbstractRedisClient client;
		protected final RedisCodec<K, V> codec;

		protected WriterOptions options = WriterOptions.builder().build();

		protected BaseBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		@SuppressWarnings("unchecked")
		public B options(WriterOptions options) {
			this.options = options;
			return (B) this;
		}

		protected void configure(AbstractRedisItemWriter<K, V, ?> writer) {
			writer.setOptions(options);
		}

	}

	public static class Builder<K, V> extends BaseBuilder<K, V, Builder<K, V>> {

		private StructOptions structOptions = StructOptions.builder().build();

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public Builder<K, V> structOptions(StructOptions options) {
			this.structOptions = options;
			return this;
		}

		public RedisItemWriter<K, V> dump() {
			return build(ValueType.DUMP);
		}

		public RedisItemWriter<K, V> build(ValueType type) {
			RedisItemWriter<K, V> writer = new RedisItemWriter<>(client, codec, type);
			configure(writer);
			return writer;
		}

		public RedisItemWriter<K, V> struct() {
			RedisItemWriter<K, V> writer = build(ValueType.STRUCT);
			writer.setStructOptions(structOptions);
			return writer;
		}

	}

}
