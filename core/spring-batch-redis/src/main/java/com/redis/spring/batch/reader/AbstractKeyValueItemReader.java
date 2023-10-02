package com.redis.spring.batch.reader;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.OperationItemProcessor;
import com.redis.spring.batch.common.SimpleBatchOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractKeyValueItemReader<K, V> extends RedisItemReader<K, V, KeyValue<K>> {

    protected AbstractKeyValueItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    @Override
    protected ItemProcessor<List<? extends K>, List<KeyValue<K>>> processor() {
        return operationProcessor();
    }

    public OperationItemProcessor<K, V, K, KeyValue<K>> operationProcessor() {
        SimpleBatchOperation<K, V, K, KeyValue<K>> batchOperation = new SimpleBatchOperation<>(operation());
        OperationItemProcessor<K, V, K, KeyValue<K>> executor = new OperationItemProcessor<>(client, codec, batchOperation);
        executor.setPoolSize(poolSize);
        executor.setReadFrom(readFrom);
        return executor;
    }

    protected abstract Operation<K, V, K, KeyValue<K>> operation();

}
