package com.redis.spring.batch.reader;

import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class Evalsha<K, V> implements Operation<K, V, K, List<Object>> {

    private final String digest;

    private final V[] args;

    public Evalsha(RedisCodec<K, V> codec, String digest, Object... args) {
        this.digest = digest;
        this.args = encode(codec, args);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, K item, List<RedisFuture<List<Object>>> futures) {
        K[] keys = (K[]) new Object[] { item };
        futures.add(((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest, ScriptOutputType.MULTI, keys, args));
    }

    @SuppressWarnings("unchecked")
    private static <K, V> V[] encode(RedisCodec<K, V> codec, Object... values) {
        Function<String, V> stringValueFunction = CodecUtils.stringValueFunction(codec);
        Object[] encodedValues = new Object[values.length];
        for (int index = 0; index < values.length; index++) {
            encodedValues[index] = stringValueFunction.apply(String.valueOf(values[index]));
        }
        return (V[]) encodedValues;
    }

}
