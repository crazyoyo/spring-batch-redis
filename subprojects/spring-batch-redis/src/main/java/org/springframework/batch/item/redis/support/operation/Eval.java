package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Eval<K, V, T> implements RedisOperation<K, V, T> {

    private final String sha;
    private final ScriptOutputType output;
    private final Converter<T, K[]> keys;
    private final Converter<T, V[]> args;

    public Eval(String sha, ScriptOutputType output, Converter<T, K[]> keys, Converter<T, V[]> args) {
        Assert.notNull(sha, "A SHA digest is required");
        Assert.notNull(output, "A script output type is required");
        Assert.notNull(keys, "A keys converter is required");
        Assert.notNull(args, "An args converter is required");
        this.sha = sha;
        this.output = output;
        this.keys = keys;
        this.args = args;
    }

    @Override
    public RedisFuture<?> execute(RedisModulesAsyncCommands<K, V> commands, T item) {
        return commands.evalsha(sha, output, keys.convert(item), args.convert(item));
    }

}
