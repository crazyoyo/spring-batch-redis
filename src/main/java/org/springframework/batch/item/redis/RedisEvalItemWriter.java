package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public class RedisEvalItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

	private final String sha;
	private final ScriptOutputType outputType;
	private final Converter<T, K[]> keysConverter;
	private final Converter<T, V[]> argsConverter;

	public RedisEvalItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
			Converter<T, K[]> keysConverter, String sha, Converter<T, V[]> argsConverter, ScriptOutputType outputType) {
		super(pool, commands, commandTimeout);
		this.sha = sha;
		this.outputType = outputType;
		this.keysConverter = keysConverter;
		this.argsConverter = argsConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
		return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, outputType, keysConverter.convert(item),
				argsConverter.convert(item));
	}

}
