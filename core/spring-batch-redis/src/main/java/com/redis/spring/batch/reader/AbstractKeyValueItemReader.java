package com.redis.spring.batch.reader;

import java.util.function.Function;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.reader.operation.Evalsha;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractKeyValueItemReader<K, V, T extends KeyValue<K>> extends RedisItemReader<K, V, T> {

    private static final String LUA_FILENAME = "keyvalue.lua";

    /**
     * Default to no memory usage calculation
     */
    public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = DataSize.ofBytes(0);

    public static final int DEFAULT_MEMORY_USAGE_SAMPLES = 5;

    protected DataSize memoryUsageLimit = DEFAULT_MEMORY_USAGE_LIMIT;

    protected int memoryUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

    protected AbstractKeyValueItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    public void setMemoryUsageLimit(DataSize limit) {
        this.memoryUsageLimit = limit;
    }

    public void setMemoryUsageSamples(int samples) {
        this.memoryUsageSamples = samples;
    }

    @Override
    protected Operation<K, V, K, T> operation() {
        Object[] args = { memoryUsageLimit.toBytes(), memoryUsageSamples, typeName() };
        Evalsha<K, V, K> evalsha = new Evalsha<>(LUA_FILENAME, client, codec, args);
        evalsha.setKeyFunction(Function.identity());
        return operation(evalsha);
    }

    protected abstract Operation<K, V, K, T> operation(Evalsha<K, V, K> evalsha);

    protected abstract String typeName();

}
