package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;

import java.time.Duration;
import java.util.function.Function;

public abstract class AbstractKeyItemWriter<K, V, T> extends AbstractCommandItemWriter<K, V, T> {

    private final Converter<T, K> keyConverter;

    public AbstractKeyItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter) {
        super(pool, commands, commandTimeout);
        this.keyConverter = keyConverter;
    }

    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
        return write(commands, item, keyConverter.convert(item));
    }

    protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key);
}
