package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionPoolBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class EvalItemWriter<T> extends AbstractRedisItemWriter<T> {

	private final String sha;
	private final ScriptOutputType outputType;
	private final Converter<T, String[]> keysConverter;
	private final Converter<T, String[]> argsConverter;

	public EvalItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, String sha,
			ScriptOutputType outputType, Converter<T, String[]> keysConverter, Converter<T, String[]> argsConverter) {
		super(client, poolConfig);
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

	public static <T> EvalItemWriterBuilder<T> builder() {
		return new EvalItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class EvalItemWriterBuilder<T> extends RedisConnectionPoolBuilder<EvalItemWriterBuilder<T>> {

		private String sha;
		private ScriptOutputType outputType;
		private Converter<T, String[]> keysConverter;
		private Converter<T, String[]> argsConverter;

		public EvalItemWriter<T> build() {
			return new EvalItemWriter<>(client, poolConfig, sha, outputType, keysConverter, argsConverter);
		}

	}

}
