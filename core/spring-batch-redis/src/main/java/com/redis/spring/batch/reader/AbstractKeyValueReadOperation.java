package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.CollectionUtils;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractKeyValueReadOperation<K, V, T extends KeyValue<K, ?>> extends ItemStreamSupport
        implements Operation<K, V, K, T> {

    /**
     * Default to no memory usage calculation
     */
    public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = DataSize.ofBytes(0);

    public static final int DEFAULT_MEMORY_USAGE_SAMPLES = 5;

    private static final String LUA_FILENAME = "keyvalue.lua";

    private final String typeName;

    private DataSize memoryUsageLimit = DEFAULT_MEMORY_USAGE_LIMIT;

    private int memoryUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

    private final AbstractRedisClient client;

    private RedisCodec<K, V> codec;

    private Evalsha<K, V> evalsha;

    protected AbstractKeyValueReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec, String typeName) {
        this.client = client;
        this.codec = codec;
        this.typeName = typeName;
    }

    public void setMemoryUsageLimit(DataSize limit) {
        this.memoryUsageLimit = limit;
    }

    public void setMemoryUsageSamples(int samples) {
        this.memoryUsageSamples = samples;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        Object[] args = { memoryUsageLimit.toBytes(), memoryUsageSamples, typeName };
        this.evalsha = new Evalsha<>(LUA_FILENAME, client, codec, args);
        super.open(executionContext);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, K item, List<RedisFuture<T>> futures) {
        RedisFuture<List<Object>> future = evalsha.execute(commands, item);
        futures.add(new MappingRedisFuture<>(future.toCompletableFuture(), this::toKeyValue));
    }

    protected T toKeyValue(List<Object> list) {
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        return keyValue(list.iterator());
    }

    @SuppressWarnings("unchecked")
    private T keyValue(Iterator<Object> iterator) {
        if (!iterator.hasNext()) {
            return null;
        }
        K key = (K) iterator.next();
        long ttl = ttl(iterator);
        DataSize memoryUsage = memoryUsage(iterator);
        T keyValue = keyValue(key, iterator);
        keyValue.setTtl(ttl);
        keyValue.setMemoryUsage(memoryUsage);
        return keyValue;
    }

    private long ttl(Iterator<Object> iterator) {
        if (iterator.hasNext()) {
            return (Long) iterator.next();
        }
        return 0;
    }

    private DataSize memoryUsage(Iterator<Object> iterator) {
        if (iterator.hasNext()) {
            return DataSize.ofBytes((Long) iterator.next());
        }
        return null;
    }

    protected abstract T keyValue(K key, Iterator<Object> iterator);

}
