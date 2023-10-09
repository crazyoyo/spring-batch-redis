package com.redis.spring.batch.reader;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.OperationValueReader;
import com.redis.spring.batch.common.SimpleBatchOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class KeyValueItemReader<K, V> extends RedisItemReader<K, V, KeyValue<K>> {

    /**
     * Default to no memory usage calculation
     */
    public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = DataSize.ofBytes(0);

    public static final int DEFAULT_MEMORY_USAGE_SAMPLES = 5;

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    private int poolSize = DEFAULT_POOL_SIZE;

    protected DataSize memLimit = DEFAULT_MEMORY_USAGE_LIMIT;

    protected int memSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

    public void setMemoryUsageLimit(DataSize limit) {
        this.memLimit = limit;
    }

    public void setMemoryUsageSamples(int samples) {
        this.memSamples = samples;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    protected KeyValueItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    @Override
    protected ValueReader<K, KeyValue<K>> valueReader() {
        return operationValueReader();
    }

    public OperationValueReader<K, V, K, KeyValue<K>> operationValueReader() {
        SimpleBatchOperation<K, V, K, KeyValue<K>> operation = new SimpleBatchOperation<>(operation());
        OperationValueReader<K, V, K, KeyValue<K>> executor = new OperationValueReader<>(getClient(), getCodec(), operation);
        executor.setPoolSize(poolSize);
        executor.setReadFrom(getReadFrom());
        return executor;
    }

    protected abstract Operation<K, V, K, KeyValue<K>> operation();

}
