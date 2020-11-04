package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisNoOpItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

    public RedisNoOpItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout) {
	super(pool, commands, commandTimeout);
    }

    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
	return null;
    }

    public static <T> RedisNoOpItemWriterBuilder<String, String, T> builder() {
	return new RedisNoOpItemWriterBuilder<>(StringCodec.UTF8);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisNoOpItemWriterBuilder<K, V, T>
	    extends RedisConnectionBuilder<K, V, RedisNoOpItemWriterBuilder<K, V, T>> {

	public RedisNoOpItemWriterBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisNoOpItemWriter<K, V, T> build() {
	    return new RedisNoOpItemWriter<>(pool(), async(), timeout());
	}

    }

}
