package org.springframework.batch.item.redis;

import java.time.Duration;
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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisStringItemWriter<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

    private final Converter<T, V> valueConverter;

    public RedisStringItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
	    Converter<T, K> keyConverter, Converter<T, V> valueConverter) {
	super(pool, commands, commandTimeout, keyConverter);
	Assert.notNull(valueConverter, "A value converter is required.");
	this.valueConverter = valueConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, T item) {
	return ((RedisStringAsyncCommands<K, V>) commands).set(key, valueConverter.convert(item));
    }

    public static <T> RedisStringItemWriterBuilder<String, String, T> builder() {
	return new RedisStringItemWriterBuilder<>(StringCodec.UTF8);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisStringItemWriterBuilder<K, V, T>
	    extends AbstractKeyCommandItemWriterBuilder<K, V, T, RedisStringItemWriterBuilder<K, V, T>> {

	private Converter<T, V> valueConverter;

	public RedisStringItemWriterBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisStringItemWriter<K, V, T> build() {
	    return new RedisStringItemWriter<>(pool(), async(), timeout(), keyConverter, valueConverter);
	}

    }

}
