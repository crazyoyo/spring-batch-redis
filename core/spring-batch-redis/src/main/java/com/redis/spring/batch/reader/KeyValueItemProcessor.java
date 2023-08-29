package com.redis.spring.batch.reader;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.AbstractRedisItemStreamSupport;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class KeyValueItemProcessor<K, V> extends AbstractRedisItemStreamSupport<K, V, K>
        implements ItemProcessor<Collection<? extends K>, List<? extends KeyValue<K>>> {

    private static final String FILENAME = "keyvalue.lua";

    private final String digest;

    private final Function<String, V> stringValueFunction;

    private ReadFrom readFrom;

    private ValueType valueType = ValueType.DUMP;

    private DataSize memoryUsageLimit;

    private int memoryUsageSamples = RedisItemReader.DEFAULT_MEMORY_USAGE_SAMPLES;

    private V[] args;

    public KeyValueItemProcessor(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
        this.digest = ConnectionUtils.loadScript(client, FILENAME);
        this.stringValueFunction = CodecUtils.stringValueFunction(codec);
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public DataSize getMemoryUsageLimit() {
        return memoryUsageLimit;
    }

    public void setMemoryUsageLimit(DataSize memoryUsageLimit) {
        this.memoryUsageLimit = memoryUsageLimit;
    }

    public int getMemoryUsageSamples() {
        return memoryUsageSamples;
    }

    public void setMemoryUsageSamples(int memoryUsageSamples) {
        this.memoryUsageSamples = memoryUsageSamples;
    }

    @Override
    protected Supplier<StatefulConnection<K, V>> connectionSupplier() {
        return ConnectionUtils.supplier(client, codec, readFrom);
    }

    @Override
    protected void doOpen() {
        super.doOpen();
        this.args = encode(memoryUsageLimit == null ? 0 : memoryUsageLimit.toBytes(), memoryUsageSamples, valueType.name());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, K item, List<RedisFuture<?>> futures) {
        Object[] keys = { item };
        futures.add(((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest, ScriptOutputType.MULTI, (K[]) keys, args));
    }

    @SuppressWarnings("unchecked")
    public List<KeyValue<K>> process(K... keys) throws Exception {
        return process(Arrays.asList(keys));
    }

    @Override
    public List<KeyValue<K>> process(Collection<? extends K> keys) throws Exception {
        return execute(keys).stream().map(toKeyValueFunction()).collect(Collectors.toList());
    }

    private Function<Object, KeyValue<K>> toKeyValueFunction() {
        ListToKeyValue<K, V> kvProc = new ListToKeyValue<>(codec);
        if (valueType == ValueType.STRUCT) {
            return kvProc.andThen(new ListToKeyValueStruct<>(codec));
        }
        return kvProc;
    }

    @SuppressWarnings("unchecked")
    private V[] encode(Object... values) {
        Object[] encodedValues = new Object[values.length];
        for (int index = 0; index < values.length; index++) {
            encodedValues[index] = stringValueFunction.apply(String.valueOf(values[index]));
        }
        return (V[]) encodedValues;
    }

}
