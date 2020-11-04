package org.springframework.batch.item.redis;

import java.time.Duration;
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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisHashItemWriter<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

    private final Converter<T, Map<K, V>> mapConverter;

    public RedisHashItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
	    Converter<T, K> keyConverter, Converter<T, Map<K, V>> mapConverter) {
	super(pool, commands, commandTimeout, keyConverter);
	Assert.notNull(mapConverter, "A map converter is required.");
	this.mapConverter = mapConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, T item) {
	return ((RedisHashAsyncCommands<K, V>) commands).hmset(key, mapConverter.convert(item));
    }

    public static <T> RedisHashItemWriterBuilder<String, String, T> builder() {
	return new RedisHashItemWriterBuilder<>(StringCodec.UTF8);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisHashItemWriterBuilder<K, V, T>
	    extends AbstractKeyCommandItemWriterBuilder<K, V, T, RedisHashItemWriterBuilder<K, V, T>> {

	private Converter<T, Map<K, V>> mapConverter;

	public RedisHashItemWriterBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisHashItemWriter<K, V, T> build() {
	    return new RedisHashItemWriter<>(pool(), async(), timeout(), keyConverter, mapConverter);
	}

    }

}
