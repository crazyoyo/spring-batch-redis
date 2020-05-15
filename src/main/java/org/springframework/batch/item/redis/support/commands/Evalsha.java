package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import lombok.Builder;
import org.springframework.batch.item.redis.support.WriteCommand;

@Builder
public class Evalsha<K, V> implements WriteCommand<K, V, EvalshaArgs<K, V>> {

    private final String sha;
    private final ScriptOutputType outputType;

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, EvalshaArgs<K, V> args) {
        return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, outputType, args.getKeys(), args.getArgs());
    }


}