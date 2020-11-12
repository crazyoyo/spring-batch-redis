package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisNoOpItemWriter<T> extends AbstractRedisItemWriter<T> {

    public RedisNoOpItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout) {
	super(pool, commands, commandTimeout);
    }

    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, T item) {
	return null;
    }

    public static <T> RedisNoOpItemWriterBuilder<T> builder() {
	return new RedisNoOpItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisNoOpItemWriterBuilder<T> extends RedisConnectionBuilder<RedisNoOpItemWriterBuilder<T>> {

	public RedisNoOpItemWriter<T> build() {
	    return new RedisNoOpItemWriter<>(pool(), async(), timeout());
	}

    }

}
