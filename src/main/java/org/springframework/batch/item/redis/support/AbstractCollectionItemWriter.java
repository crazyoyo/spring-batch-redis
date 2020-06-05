package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Function;

public abstract class AbstractCollectionItemWriter<K, V, C extends StatefulConnection<K, V>, T> extends AbstractKeyItemWriter<K, V, C, T> {

    private final Converter<T, V> memberIdConverter;

    public AbstractCollectionItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
        super(pool, commands, commandTimeout, keyConverter);
        this.memberIdConverter = memberIdConverter;
    }

    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return write(commands, item, key, memberIdConverter.convert(item));
    }

    protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId);
}
