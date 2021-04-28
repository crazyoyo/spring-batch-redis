package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class Geoadd<T> extends AbstractCollectionOperation<T> {

    @NonNull
    private final Converter<T, Double> longitude;
    @NonNull
    private final Converter<T, Double> latitude;

    @Builder
    public Geoadd(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
        super(key, member);
        Assert.notNull(longitude, "A longitude converter is required");
        Assert.notNull(latitude, "A latitude converter is required");
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        Double lon = longitude.convert(item);
        if (lon == null) {
            return null;
        }
        Double lat = latitude.convert(item);
        if (lat == null) {
            return null;
        }
        return ((RedisGeoAsyncCommands<String, String>) commands).geoadd(key.convert(item), lon, lat, member.convert(item));
    }

}
