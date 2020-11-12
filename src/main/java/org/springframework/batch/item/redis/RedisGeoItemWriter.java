package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisGeoItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

    private final Converter<T, Double> longitudeConverter;
    private final Converter<T, Double> latitudeConverter;

    public RedisGeoItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout, Converter<T, String> keyConverter, Converter<T, String> memberIdConverter,
	    Converter<T, Double> longitudeConverter, Converter<T, Double> latitudeConverter) {
	super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
	Assert.notNull(longitudeConverter, "A longitude converter is required.");
	Assert.notNull(latitudeConverter, "A latitude converter is required.");
	this.longitudeConverter = longitudeConverter;
	this.latitudeConverter = latitudeConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, String memberId,
	    T item) {
	Double longitude = longitudeConverter.convert(item);
	if (longitude == null) {
	    return null;
	}
	Double latitude = latitudeConverter.convert(item);
	if (latitude == null) {
	    return null;
	}
	return ((RedisGeoAsyncCommands<String, String>) commands).geoadd(key, longitude, latitude, memberId);
    }

    public static <T> RedisGeoItemWriterBuilder<T> builder() {
	return new RedisGeoItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisGeoItemWriterBuilder<T>
	    extends AbstractCollectionCommandItemWriterBuilder<T, RedisGeoItemWriterBuilder<T>> {

	private Converter<T, Double> longitudeConverter;
	private Converter<T, Double> latitudeConverter;

	public RedisGeoItemWriter<T> build() {
	    return new RedisGeoItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter,
		    longitudeConverter, latitudeConverter);
	}

    }

}
