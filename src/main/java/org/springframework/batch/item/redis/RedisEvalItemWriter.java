package org.springframework.batch.item.redis;

import java.time.Duration;
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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisEvalItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

    private final String sha;

    private final ScriptOutputType outputType;

    private final Converter<T, K[]> keysConverter;

    private final Converter<T, V[]> argsConverter;

    public RedisEvalItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
	    String sha, ScriptOutputType outputType, Converter<T, K[]> keysConverter, Converter<T, V[]> argsConverter) {
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
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
	K[] keys = keysConverter.convert(item);
	V[] args = argsConverter.convert(item);
	return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, outputType, keys, args);
    }

    public static <T> RedisEvalItemWriterBuilder<String, String, T> builder() {
	return new RedisEvalItemWriterBuilder<>(StringCodec.UTF8);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisEvalItemWriterBuilder<K, V, T>
	    extends RedisConnectionBuilder<K, V, RedisEvalItemWriterBuilder<K, V, T>> {

	private String sha;

	private ScriptOutputType outputType;

	private Converter<T, K[]> keysConverter;

	private Converter<T, V[]> argsConverter;

	public RedisEvalItemWriterBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisEvalItemWriter<K, V, T> build() {
	    return new RedisEvalItemWriter<>(pool(), async(), timeout(), sha, outputType, keysConverter, argsConverter);
	}

    }

}
