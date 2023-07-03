package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyDump;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyDumpReadOperation extends AbstractLuaReadOperation<byte[], byte[], KeyDump<byte[]>> {

	private static final String FILENAME = "keydump.lua";

	public KeyDumpReadOperation(AbstractRedisClient client) {
		super(client, FILENAME);
	}

	@Override
	protected KeyDump<byte[]> keyValue() {
		return new KeyDump<>();
	}

	@Override
	protected String string(Object object) {
		return StringCodec.UTF8.decodeValue(ByteArrayCodec.INSTANCE.encodeValue((byte[]) object));
	}

	@Override
	protected void setValue(KeyDump<byte[]> keyValue, Object value) {
		keyValue.setDump((byte[]) value);
	}

	@Override
	protected byte[] encodeValue(String value) {
		return ByteArrayCodec.INSTANCE.decodeValue(StringCodec.UTF8.encodeValue(value));
	}

}
