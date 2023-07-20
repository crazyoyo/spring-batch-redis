package com.redis.spring.batch;

import java.util.function.ToLongFunction;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.ValueType;
import com.redis.spring.batch.writer.AbstractOperationItemWriter;
import com.redis.spring.batch.writer.TtlPolicy;
import com.redis.spring.batch.writer.StructWriteOperation;
import com.redis.spring.batch.writer.WriteOperation;
import com.redis.spring.batch.writer.WriteOperationOptions;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.RestoreReplace;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V> extends AbstractOperationItemWriter<K, V, KeyValue<K>> {

    private final ValueType valueType;

    private WriterOptions writerOptions = WriterOptions.builder().build();

    public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, ValueType valueType) {
        super(client, codec);
        this.valueType = valueType;
    }

    public WriterOptions getWriterOptions() {
        return writerOptions;
    }

    public void setWriterOptions(WriterOptions options) {
        this.writerOptions = options;
    }

    public ValueType getValueType() {
        return valueType;
    }

    @Override
    protected WriteOperation<K, V, KeyValue<K>> operation() {
        if (valueType == ValueType.DUMP) {
            return new RestoreReplace<>(KeyValue::getKey, v -> (byte[]) v.getValue(), keyValueTtl());
        }
        StructWriteOperation<K, V> operation = new StructWriteOperation<>();
        operation.setOptions(writerOptions);
        return operation;
    }

    private ToLongFunction<KeyValue<K>> keyValueTtl() {
        if (writerOptions.getTtlPolicy() == TtlPolicy.PROPAGATE) {
            return KeyValue::getTtl;
        }
        return kv -> 0;
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

        private WriteOperationOptions operationOptions = WriteOperationOptions.builder().build();

        protected BaseBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
            this.client = client;
            this.codec = codec;
        }

        @SuppressWarnings("unchecked")
        public B operationOptions(WriteOperationOptions options) {
            this.operationOptions = options;
            return (B) this;
        }

        protected void configure(AbstractOperationItemWriter<K, V, ?> writer) {
            writer.setOptions(operationOptions);
        }

    }

    public static class Builder<K, V> extends BaseBuilder<K, V, Builder<K, V>> {

        private WriterOptions writerOptions = WriterOptions.builder().build();

        public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
            super(client, codec);
        }

        public Builder<K, V> writerOptions(WriterOptions options) {
            this.writerOptions = options;
            return this;
        }

        public RedisItemWriter<K, V> dump() {
            return build(ValueType.DUMP);
        }

        public RedisItemWriter<K, V> build(ValueType type) {
            RedisItemWriter<K, V> writer = new RedisItemWriter<>(client, codec, type);
            configure(writer);
            writer.setWriterOptions(writerOptions);
            return writer;
        }

        public RedisItemWriter<K, V> struct() {
            return build(ValueType.STRUCT);
        }

    }

}
