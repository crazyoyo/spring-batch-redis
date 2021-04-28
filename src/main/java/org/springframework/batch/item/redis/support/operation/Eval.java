package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import lombok.Builder;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class Eval<T> implements RedisOperation<String, String, T> {

    private final String sha;
    private final ScriptOutputType output;
    private final Converter<T, String[]> keys;
    private final Converter<T, String[]> args;

    @Builder
    public Eval(String sha, ScriptOutputType output, Converter<T, String[]> keys, Converter<T, String[]> args) {
        Assert.notNull(sha, "A SHA digest converter is required");
        Assert.notNull(output, "A script output type converter is required");
        Assert.notNull(keys, "A keys converter is required");
        Assert.notNull(args, "An args converter is required");
        this.sha = sha;
        this.output = output;
        this.keys = keys;
        this.args = args;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisScriptingAsyncCommands<String, String>) commands).evalsha(sha, output, keys.convert(item), args.convert(item));
    }

}
