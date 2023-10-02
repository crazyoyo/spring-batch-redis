package com.redis.spring.batch.reader;

import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class Evalsha<K, V, T> implements Operation<K, V, T, List<Object>> {

    private final String digest;

    private final Function<String, V> stringValueFunction;

    private Function<T, K> keyFunction;

    private Function<T, V[]> argsFunction;

    public Evalsha(String filename, AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.digest = ConnectionUtils.loadScript(client, filename);
        this.stringValueFunction = CodecUtils.stringValueFunction(codec);
    }

    public void setKeys(K key) {
        setKeyFunction(t -> key);
    }

    public void setKeyFunction(Function<T, K> function) {
        this.keyFunction = function;
    }

    public void setArgsFunction(Function<T, V[]> function) {
        this.argsFunction = function;
    }

    @SuppressWarnings("unchecked")
    public void setArgs(Object... args) {
        V[] encodedArgs = (V[]) new Object[args.length];
        for (int index = 0; index < args.length; index++) {
            encodedArgs[index] = stringValueFunction.apply(String.valueOf(args[index]));
        }
        setArgsFunction(t -> encodedArgs);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<List<Object>> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        K[] keys = (K[]) new Object[] { keyFunction.apply(item) };
        V[] args = argsFunction.apply(item);
        return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest, ScriptOutputType.MULTI, keys, args);
    }

}
