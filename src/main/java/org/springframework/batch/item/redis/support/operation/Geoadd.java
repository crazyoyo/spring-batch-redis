package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Geoadd<T> extends AbstractCollectionOperation<T> {

    private final Converter<T, Double> longitude;
    private final Converter<T, Double> latitude;

    public Geoadd(String key, Converter<T, String> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
        this(new ConstantConverter<>(key), member, longitude, latitude);
    }

    public Geoadd(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
        this(key, member, new ConstantPredicate<>(false), new NullValuePredicate<>(longitude), longitude, latitude);
    }

    public Geoadd(Converter<T, String> key, Converter<T, String> member, Predicate<T> delete, Predicate<T> remove, Converter<T, Double> longitude, Converter<T, Double> latitude) {
        super(key, member, delete, remove);
        Assert.notNull(longitude, "A longitude converter is required");
        Assert.notNull(latitude, "A latitude converter is required");
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<?> add(BaseRedisAsyncCommands<String, String> commands, T item, String key, String member) {
        Double lon = longitude.convert(item);
        if (lon == null) {
            return null;
        }
        Double lat = latitude.convert(item);
        if (lat == null) {
            return null;
        }
        return ((RedisGeoAsyncCommands<String, String>) commands).geoadd(key, lon, lat, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<String, String> commands, String key, String member) {
        return ((RedisSortedSetAsyncCommands<String, String>) commands).zrem(key, member);
    }

}
