package com.redis.spring.batch;

import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.IteratorItemReader;

import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.reader.AbstractRedisItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.LuaToDumpFunction;
import com.redis.spring.batch.reader.LuaToKeyValueFunction;
import com.redis.spring.batch.reader.LuaToStructFunction;
import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemReader<K, V, T extends KeyValue<K, ?>> extends AbstractRedisItemReader<K, V, T> {

    public static final int DEFAULT_SCAN_COUNT = DEFAULT_CHUNK_SIZE;

    private long scanCount = DEFAULT_SCAN_COUNT;

    public RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, LuaToKeyValueFunction<T> lua) {
        super(client, codec, lua);
    }

    public void setScanCount(long count) {
        this.scanCount = count;
    }

    @Override
    protected ItemReader<K> keyReader() {
        Supplier<StatefulConnection<K, V>> supplier = ConnectionUtils.supplier(client, codec, readFrom);
        StatefulConnection<K, V> connection = supplier.get();
        ScanIterator<K> iterator = ScanIterator.scan(ConnectionUtils.sync(connection), scanArgs());
        return new IteratorItemReader<>(iterator);
    }

    private KeyScanArgs scanArgs() {
        KeyScanArgs args = new KeyScanArgs();
        args.limit(scanCount);
        if (keyPattern != null) {
            args.match(keyPattern);
        }
        if (keyType != null) {
            args.type(keyType);
        }
        return args;
    }

    public static <K, V> RedisItemReader<K, V, Struct<K>> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new RedisItemReader<>(client, codec, new LuaToStructFunction<>(codec));
    }

    public static <K, V> RedisItemReader<K, V, Dump<K>> dump(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new RedisItemReader<>(client, codec, new LuaToDumpFunction<>());
    }

    public static <K, V> LiveRedisItemReader<K, V, Struct<K>> liveStruct(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new LiveRedisItemReader<>(client, codec, new LuaToStructFunction<>(codec));
    }

    public static <K, V> LiveRedisItemReader<K, V, Dump<K>> liveDump(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new LiveRedisItemReader<>(client, codec, new LuaToDumpFunction<>());
    }

}
