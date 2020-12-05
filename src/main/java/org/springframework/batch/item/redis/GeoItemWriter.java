package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class GeoItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

	private final Converter<T, Double> longitudeConverter;
	private final Converter<T, Double> latitudeConverter;

	public GeoItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, String> memberIdConverter, Converter<T, Double> longitudeConverter,
			Converter<T, Double> latitudeConverter) {
		super(client, poolConfig, keyConverter, memberIdConverter);
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

	public static <T> GeoItemWriterBuilder<T> builder() {
		return new GeoItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class GeoItemWriterBuilder<T>
			extends AbstractCollectionCommandItemWriterBuilder<T, GeoItemWriterBuilder<T>> {

		private Converter<T, Double> longitudeConverter;
		private Converter<T, Double> latitudeConverter;

		public GeoItemWriter<T> build() {
			return new GeoItemWriter<>(client, poolConfig, keyConverter, memberIdConverter, longitudeConverter,
					latitudeConverter);
		}

	}

}
