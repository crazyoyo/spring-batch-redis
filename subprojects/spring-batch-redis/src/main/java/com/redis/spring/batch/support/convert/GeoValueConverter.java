package com.redis.spring.batch.support.convert;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.GeoValue;

public class GeoValueConverter<V, T> implements Converter<T, GeoValue<V>> {

	private final Converter<T, V> member;
	private final Converter<T, Double> longitude;
	private final Converter<T, Double> latitude;

	public GeoValueConverter(Converter<T, V> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
		this.member = member;
		this.longitude = longitude;
		this.latitude = latitude;
	}

	@Override
	public GeoValue<V> convert(T source) {
		Double longitude = this.longitude.convert(source);
		if (longitude == null) {
			return null;
		}
		Double latitude = this.latitude.convert(source);
		if (latitude == null) {
			return null;
		}
		return GeoValue.just(longitude, latitude, member.convert(source));
	}

}