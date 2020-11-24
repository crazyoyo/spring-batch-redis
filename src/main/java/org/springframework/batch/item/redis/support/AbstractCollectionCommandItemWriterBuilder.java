package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

public abstract class AbstractCollectionCommandItemWriterBuilder<T, B extends AbstractCollectionCommandItemWriterBuilder<T, B>>
		extends AbstractKeyCommandItemWriterBuilder<T, B> {

	protected Converter<T, String> memberIdConverter;

	@SuppressWarnings("unchecked")
	public B memberIdConverter(Converter<T, String> memberIdConverter) {
		this.memberIdConverter = memberIdConverter;
		return (B) this;
	}

}
