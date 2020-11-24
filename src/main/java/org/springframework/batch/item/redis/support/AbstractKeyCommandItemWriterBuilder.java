package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

public abstract class AbstractKeyCommandItemWriterBuilder<T, B extends AbstractKeyCommandItemWriterBuilder<T, B>>
		extends RedisConnectionBuilder<B> {

	protected Converter<T, String> keyConverter;

	@SuppressWarnings("unchecked")
	public B keyConverter(Converter<T, String> keyConverter) {
		this.keyConverter = keyConverter;
		return (B) this;
	}

}
