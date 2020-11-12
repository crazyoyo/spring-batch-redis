package org.springframework.batch.item.redis;

import java.util.Map;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisHashItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

    private final Converter<T, Map<String, String>> mapConverter;

    public RedisHashItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout, Converter<T, String> keyConverter, Converter<T, Map<String, String>> mapConverter) {
	super(pool, commands, commandTimeout, keyConverter);
	Assert.notNull(mapConverter, "A map converter is required.");
	this.mapConverter = mapConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
	return ((RedisHashAsyncCommands<String, String>) commands).hmset(key, mapConverter.convert(item));
    }

    public static <T> RedisHashItemWriterBuilder<T> builder() {
	return new RedisHashItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisHashItemWriterBuilder<T>
	    extends AbstractKeyCommandItemWriterBuilder<T, RedisHashItemWriterBuilder<T>> {

	private Converter<T, Map<String, String>> mapConverter;

	public RedisHashItemWriter<T> build() {
	    return new RedisHashItemWriter<>(pool(), async(), timeout(), keyConverter, mapConverter);
	}

    }

}
