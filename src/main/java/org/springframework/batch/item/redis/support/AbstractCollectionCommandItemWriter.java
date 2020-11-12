package org.springframework.batch.item.redis.support;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractCollectionCommandItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

    private final Converter<T, String> memberIdConverter;

    protected AbstractCollectionCommandItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout, Converter<T, String> keyConverter, Converter<T, String> memberIdConverter) {
	super(pool, commands, commandTimeout, keyConverter);
	Assert.notNull(memberIdConverter, "A member id converter is required.");
	this.memberIdConverter = memberIdConverter;
    }

    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
	return write(commands, key, memberIdConverter.convert(item), item);
    }

    protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key,
	    String memberId, T item);

}
