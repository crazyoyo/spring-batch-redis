package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Eval<T> implements OperationItemWriter.RedisOperation<T> {

    private final String sha;
    private final ScriptOutputType output;
    private final Converter<T, String[]> keys;
    private final Converter<T, String[]> args;

    public Eval(String sha, ScriptOutputType output, Converter<T, String[]> keys, Converter<T, String[]> args) {
        Assert.notNull(sha, "A SHA digest is required");
        Assert.notNull(output, "A script output type is required");
        Assert.notNull(keys, "A keys converter is required");
        Assert.notNull(args, "An args converter is required");
        this.sha = sha;
        this.output = output;
        this.keys = keys;
        this.args = args;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisScriptingAsyncCommands<String, String>) commands).evalsha(sha, output, keys.convert(item), args.convert(item));
    }

}
