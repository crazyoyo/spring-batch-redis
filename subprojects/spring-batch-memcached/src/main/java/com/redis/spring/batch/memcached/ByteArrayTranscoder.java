package com.redis.spring.batch.memcached;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Transcoder that serializes and unserializes byte arrays.
 */
public final class ByteArrayTranscoder extends SpyObject implements Transcoder<byte[]> {

	public boolean asyncDecode(CachedData d) {
		return false;
	}

	@Override
	public CachedData encode(byte[] o) {
		return new CachedData(0, o, getMaxSize());
	}

	@Override
	public byte[] decode(CachedData d) {
		return d.getData();
	}

	public int getMaxSize() {
		return CachedData.MAX_SIZE;
	}
}
