package com.redis.spring.batch.reader;

import java.util.function.Predicate;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

/**
 * 
 * Predicate for filtering unwanted keys that fall outside of a given slot
 * range. Returns true if key is outside of the slot range.
 * 
 * @author jruaux
 *
 * @param <K>
 */
public class KeySlotPredicate<K> implements Predicate<K> {

	private final RedisCodec<K, ?> codec;
	private final int min;
	private final int max;

	public KeySlotPredicate(RedisCodec<K, ?> codec, int min, int max) {
		this.codec = codec;
		this.min = min;
		this.max = max;
	}

	@Override
	public boolean test(K key) {
		int slot = SlotHash.getSlot(codec.encodeKey(key));
		return slot < min || slot > max;
	}

	public static KeySlotPredicate<String> of(int min, int max) {
		return new KeySlotPredicate<>(StringCodec.UTF8, min, max);
	}

	public static <K> KeySlotPredicate<K> of(RedisCodec<K, ?> codec, int min, int max) {
		return new KeySlotPredicate<>(codec, min, max);
	}

}
