package com.redis.spring.batch.convert;

import java.util.function.Function;

import io.lettuce.core.GeoValue;

public class GeoValueConverter<V, T> implements Function<T, GeoValue<V>> {

	private final Function<T, V> memberConverter;
	private final Function<T, Double> longitudeConverter;
	private final Function<T, Double> latitudeConverter;

	public GeoValueConverter(Function<T, V> member, Function<T, Double> longitude, Function<T, Double> latitude) {
		this.memberConverter = member;
		this.longitudeConverter = longitude;
		this.latitudeConverter = latitude;
	}

	@Override
	public GeoValue<V> apply(T t) {
		Double longitude = this.longitudeConverter.apply(t);
		if (longitude == null) {
			return null;
		}
		Double latitude = this.latitudeConverter.apply(t);
		if (latitude == null) {
			return null;
		}
		return GeoValue.just(longitude, latitude, memberConverter.apply(t));
	}

}