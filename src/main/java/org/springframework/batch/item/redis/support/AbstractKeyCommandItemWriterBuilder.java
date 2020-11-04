package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractKeyCommandItemWriterBuilder<K, V, T, B extends AbstractKeyCommandItemWriterBuilder<K, V, T, B>>
	extends RedisConnectionBuilder<K, V, B> {

    protected Converter<T, K> keyConverter;

    @SuppressWarnings("unchecked")
    public B keyConverter(Converter<T, K> keyConverter) {
	this.keyConverter = keyConverter;
	return (B) this;
    }

    protected AbstractKeyCommandItemWriterBuilder(RedisCodec<K, V> codec) {
	super(codec);
    }

}
