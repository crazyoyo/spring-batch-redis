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

public class Evalsha<K, V, T> implements Operation<K, V, K, T> {

    private final Function<List<Object>, T> function;

    private final String digest;

    private final V[] encodedArgs;

    @SuppressWarnings("unchecked")
    public Evalsha(String filename, AbstractRedisClient client, RedisCodec<K, V> codec, Function<List<Object>, T> function,
            Object... args) {
        this.digest = ConnectionUtils.loadScript(client, filename);
        this.function = function;
        Function<String, V> stringValueFunction = CodecUtils.stringValueFunction(codec);
        this.encodedArgs = (V[]) new Object[args.length];
        for (int index = 0; index < args.length; index++) {
            this.encodedArgs[index] = stringValueFunction.apply(String.valueOf(args[index]));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, K item, List<RedisFuture<T>> futures) {
        K[] keys = (K[]) new Object[] { item };
        RedisFuture<List<Object>> future = ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest,
                ScriptOutputType.MULTI, keys, encodedArgs);
        futures.add(new MappingRedisFuture<>(future.toCompletableFuture(), function));
    }

}
