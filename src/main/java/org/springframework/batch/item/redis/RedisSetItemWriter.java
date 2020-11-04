package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisSetItemWriter<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

    public RedisSetItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
	    Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
	super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, V memberId, T item) {
	return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, memberId);
    }

    public static <T> RedisSetItemWriterBuilder<String, String, T> builder() {
	return new RedisSetItemWriterBuilder<>(StringCodec.UTF8);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisSetItemWriterBuilder<K, V, T>
	    extends AbstractCollectionCommandItemWriterBuilder<K, V, T, RedisSetItemWriterBuilder<K, V, T>> {

	public RedisSetItemWriterBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisSetItemWriter<K, V, T> build() {
	    return new RedisSetItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter);
	}

    }

}
