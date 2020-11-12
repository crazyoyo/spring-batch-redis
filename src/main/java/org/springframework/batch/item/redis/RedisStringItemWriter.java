package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisStringItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

    private final Converter<T, String> valueConverter;

    public RedisStringItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout, Converter<T, String> keyConverter, Converter<T, String> valueConverter) {
	super(pool, commands, commandTimeout, keyConverter);
	Assert.notNull(valueConverter, "A value converter is required.");
	this.valueConverter = valueConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
	return ((RedisStringAsyncCommands<String, String>) commands).set(key, valueConverter.convert(item));
    }

    public static <T> RedisStringItemWriterBuilder<T> builder() {
	return new RedisStringItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisStringItemWriterBuilder<T>
	    extends AbstractKeyCommandItemWriterBuilder<T, RedisStringItemWriterBuilder<T>> {

	private Converter<T, String> valueConverter;

	public RedisStringItemWriter<T> build() {
	    return new RedisStringItemWriter<>(pool(), async(), timeout(), keyConverter, valueConverter);
	}

    }

}
