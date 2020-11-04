package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractCollectionCommandItemWriterBuilder<K, V, T, B extends AbstractCollectionCommandItemWriterBuilder<K, V, T, B>>
	extends AbstractKeyCommandItemWriterBuilder<K, V, T, B> {

    protected Converter<T, V> memberIdConverter;

    @SuppressWarnings("unchecked")
    public B memberIdConverter(Converter<T, V> memberIdConverter) {
	this.memberIdConverter = memberIdConverter;
	return (B) this;
    }

    protected AbstractCollectionCommandItemWriterBuilder(RedisCodec<K, V> codec) {
	super(codec);
    }

}
