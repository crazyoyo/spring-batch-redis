package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Geoadd<T> extends AbstractCollectionOperation<T> {

    private final Converter<T, Double> longitude;
    private final Converter<T, Double> latitude;

    public Geoadd(Converter<T, Object> key, Converter<T, Object> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
        this(key, t -> false, member, new NullValuePredicate<>(longitude), longitude, latitude);
    }

    public Geoadd(Converter<T, Object> key, Predicate<T> delete, Converter<T, Object> member, Predicate<T> remove, Converter<T, Double> longitude, Converter<T, Double> latitude) {
        super(key, delete, member, remove);
        Assert.notNull(longitude, "A longitude converter is required");
        Assert.notNull(latitude, "A latitude converter is required");
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        Double lon = longitude.convert(item);
        if (lon == null) {
            return null;
        }
        Double lat = latitude.convert(item);
        if (lat == null) {
            return null;
        }
        return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, lon, lat, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, member);
    }

}
