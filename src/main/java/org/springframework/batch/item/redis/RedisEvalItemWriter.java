package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisEvalItemWriter<T> extends AbstractRedisItemWriter<T> {

    private final String sha;
    private final ScriptOutputType outputType;
    private final Converter<T, String[]> keysConverter;
    private final Converter<T, String[]> argsConverter;

    public RedisEvalItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout, String sha, ScriptOutputType outputType, Converter<T, String[]> keysConverter,
	    Converter<T, String[]> argsConverter) {
	super(pool, commands, commandTimeout);
	Assert.notNull(sha, "A SHA is required.");
	Assert.notNull(outputType, "Output type is required.");
	Assert.notNull(keysConverter, "Keys converter is required.");
	Assert.notNull(argsConverter, "Args converter is required.");
	this.sha = sha;
	this.outputType = outputType;
	this.keysConverter = keysConverter;
	this.argsConverter = argsConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, T item) {
	String[] keys = keysConverter.convert(item);
	String[] args = argsConverter.convert(item);
	return ((RedisScriptingAsyncCommands<String, String>) commands).evalsha(sha, outputType, keys, args);
    }

    public static <T> RedisEvalItemWriterBuilder<T> builder() {
	return new RedisEvalItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisEvalItemWriterBuilder<T> extends RedisConnectionBuilder<RedisEvalItemWriterBuilder<T>> {

	private String sha;
	private ScriptOutputType outputType;
	private Converter<T, String[]> keysConverter;
	private Converter<T, String[]> argsConverter;

	public RedisEvalItemWriter<T> build() {
	    return new RedisEvalItemWriter<>(pool(), async(), timeout(), sha, outputType, keysConverter, argsConverter);
	}

    }

}
