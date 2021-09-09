package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

@SuppressWarnings("unchecked")
public class TsAdd<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, Long> timestamp;
    private final Converter<T, Double> value;

    public TsAdd(Converter<T, K> key, Predicate<T> delete, Converter<T, Long> timestamp, Converter<T, Double> value) {
        super(key, delete);
        Assert.notNull(timestamp, "A timestamp converter is required");
        Assert.notNull(value, "A value converter is required");
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Long timestamp = this.timestamp.convert(item);
        if (timestamp == null) {
            return null;
        }
        Double value = this.value.convert(item);
        if (value == null) {
            return null;
        }
        return ((RedisTimeSeriesAsyncCommands<K, V>) commands).add(key, timestamp, value);
    }

}
