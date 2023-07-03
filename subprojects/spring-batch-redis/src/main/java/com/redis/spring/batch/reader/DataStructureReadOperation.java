package com.redis.spring.batch.reader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class DataStructureReadOperation<K, V> extends AbstractDataStructureReadOperation<K, V> {

	private final RedisCodec<K, V> codec;

	public DataStructureReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client);
		this.codec = codec;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected String string(Object object) {
		return StringCodec.UTF8.decodeValue(codec.encodeValue((V) object));
	}

	@Override
	protected V encodeValue(String value) {
		return codec.decodeValue(StringCodec.UTF8.encodeValue(value));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <K, V> AbstractDataStructureReadOperation<K, V> of(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (codec instanceof StringCodec) {
			return (AbstractDataStructureReadOperation) new StringDataStructureReadOperation(client);
		}
		return new DataStructureReadOperation<>(client, codec);
	}

}
