package com.redis.spring.batch.reader;

import java.util.function.Predicate;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

/**
 * 
 * KeyFilter that filters out unwanted keys that fall outside of a given slot
 * range. Returns true if a given key is within the slot range.
 * 
 * @author jruaux
 *
 * @param <K>
 */
public class SlotRangeFilter<K, V> implements Predicate<K> {

	private final RedisCodec<K, V> codec;
	private final int min;
	private final int max;

	public SlotRangeFilter(RedisCodec<K, V> codec, int min, int max) {
		this.codec = codec;
		this.min = min;
		this.max = max;
	}

	@Override
	public boolean test(K key) {
		int slot = SlotHash.getSlot(codec.encodeKey(key));
		return slot >= min && slot <= max;
	}

	public static SlotRangeFilter<String, String> of(int min, int max) {
		return new SlotRangeFilter<>(StringCodec.UTF8, min, max);
	}

	public static <K, V> SlotRangeFilter<K, V> of(RedisCodec<K, V> codec, int min, int max) {
		return new SlotRangeFilter<>(codec, min, max);
	}

}
