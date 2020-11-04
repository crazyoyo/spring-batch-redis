package org.springframework.batch.item.redis;

import java.time.Duration;
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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisGeoItemWriter<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

    private final Converter<T, Double> longitudeConverter;

    private final Converter<T, Double> latitudeConverter;

    public RedisGeoItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
	    Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
	    Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> longitudeConverter,
	    Converter<T, Double> latitudeConverter) {
	super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
	Assert.notNull(longitudeConverter, "A longitude converter is required.");
	Assert.notNull(latitudeConverter, "A latitude converter is required.");
	this.longitudeConverter = longitudeConverter;
	this.latitudeConverter = latitudeConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, V memberId, T item) {
	Double longitude = longitudeConverter.convert(item);
	if (longitude == null) {
	    return null;
	}
	Double latitude = latitudeConverter.convert(item);
	if (latitude == null) {
	    return null;
	}
	return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, longitude, latitude, memberId);
    }

    public static <T> RedisGeoItemWriterBuilder<String, String, T> builder() {
	return new RedisGeoItemWriterBuilder<>(StringCodec.UTF8);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisGeoItemWriterBuilder<K, V, T>
	    extends AbstractCollectionCommandItemWriterBuilder<K, V, T, RedisGeoItemWriterBuilder<K, V, T>> {

	private Converter<T, Double> longitudeConverter;

	private Converter<T, Double> latitudeConverter;

	public RedisGeoItemWriterBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisGeoItemWriter<K, V, T> build() {
	    return new RedisGeoItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter,
		    longitudeConverter, latitudeConverter);
	}

    }

}
