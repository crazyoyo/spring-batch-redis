package org.springframework.batch.item.redis;

import java.util.Map;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriterBuilder;
import org.springframework.batch.item.redis.support.ConstantConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisStreamItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

    private final Converter<T, Map<String, String>> bodyConverter;

    private final Converter<T, XAddArgs> argsConverter;

    public RedisStreamItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout, Converter<T, String> keyConverter, Converter<T, Map<String, String>> bodyConverter,
	    Converter<T, XAddArgs> argsConverter) {
	super(pool, commands, commandTimeout, keyConverter);
	Assert.notNull(bodyConverter, "Body converter is required.");
	Assert.notNull(argsConverter, "Args converter is required.");
	this.bodyConverter = bodyConverter;
	this.argsConverter = argsConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
	RedisStreamAsyncCommands<String, String> streamCommands = (RedisStreamAsyncCommands<String, String>) commands;
	XAddArgs args = argsConverter.convert(item);
	Map<String, String> body = bodyConverter.convert(item);
	return streamCommands.xadd(key, args, body);
    }

    public static <T> RedisStreamItemWriterBuilder<T> builder() {
	return new RedisStreamItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisStreamItemWriterBuilder<T>
	    extends AbstractKeyCommandItemWriterBuilder<T, RedisStreamItemWriterBuilder<T>> {

	private Converter<T, Map<String, String>> bodyConverter;

	private Converter<T, XAddArgs> argsConverter = new ConstantConverter<>(null);

	public RedisStreamItemWriter<T> build() {
	    return new RedisStreamItemWriter<>(pool(), async(), timeout(), keyConverter, bodyConverter, argsConverter);
	}

    }

}
