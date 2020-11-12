package org.springframework.batch.item.redis.support;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractKeyCommandItemWriter<T> extends AbstractRedisItemWriter<T> {

    private final Converter<T, String> keyConverter;

    protected AbstractKeyCommandItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands, long commandTimeout,
	    Converter<T, String> keyConverter) {
	super(pool, commands, commandTimeout);
	Assert.notNull(keyConverter, "A key converter is required.");
	this.keyConverter = keyConverter;
    }

    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, T item) {
	return write(commands, keyConverter.convert(item), item);
    }

    protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item);

}
