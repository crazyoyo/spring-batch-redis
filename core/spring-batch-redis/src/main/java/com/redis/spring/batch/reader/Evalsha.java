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

public class Evalsha<K, V> implements Operation<K, V, K, List<Object>> {

    private final String digest;

    private final V[] encodedArgs;

    @SuppressWarnings("unchecked")
    public Evalsha(String filename, AbstractRedisClient client, RedisCodec<K, V> codec, Object... args) {
        this.digest = ConnectionUtils.loadScript(client, filename);
        Function<String, V> stringValueFunction = CodecUtils.stringValueFunction(codec);
        this.encodedArgs = (V[]) new Object[args.length];
        for (int index = 0; index < args.length; index++) {
            this.encodedArgs[index] = stringValueFunction.apply(String.valueOf(args[index]));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, K item, List<RedisFuture<List<Object>>> futures) {
        futures.add(execute(commands, item));
    }

    @SuppressWarnings("unchecked")
    public RedisFuture<List<Object>> execute(BaseRedisAsyncCommands<K, V> commands, K... keys) {
        return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest, ScriptOutputType.MULTI, keys, encodedArgs);
    }

}
