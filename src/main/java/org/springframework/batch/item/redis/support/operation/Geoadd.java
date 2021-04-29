package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Geoadd<T> extends AbstractCollectionOperation<T> {

    private final Converter<T, Double> longitude;
    private final Converter<T, Double> latitude;

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

    public static <T> GeoaddBuilder<T> builder() {
        return new GeoaddBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class GeoaddBuilder<T> extends CollectionOperationBuilder<T, GeoaddBuilder<T>> {

        private Converter<T, Double> longitude;
        private Converter<T, Double> latitude;

        public Geoadd<T> build() {
            return new Geoadd<>(key, member, longitude, latitude);
        }

    }

}
