package com.redis.spring.batch.convert;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.GeoValue;

public class GeoValueConverter<V, T> implements Converter<T, GeoValue<V>> {

	private final Converter<T, V> memberConverter;
	private final Converter<T, Double> longitudeConverter;
	private final Converter<T, Double> latitudeConverter;

	public GeoValueConverter(Converter<T, V> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
		this.memberConverter = member;
		this.longitudeConverter = longitude;
		this.latitudeConverter = latitude;
	}

	@Override
	public GeoValue<V> convert(T source) {
		Double longitude = this.longitudeConverter.convert(source);
		if (longitude == null) {
			return null;
		}
		Double latitude = this.latitudeConverter.convert(source);
		if (latitude == null) {
			return null;
		}
		return GeoValue.just(longitude, latitude, memberConverter.convert(source));
	}

}