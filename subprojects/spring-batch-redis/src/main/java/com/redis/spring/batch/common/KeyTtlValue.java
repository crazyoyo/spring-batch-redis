package com.redis.spring.batch.common;

import java.util.Objects;

public class KeyTtlValue<K> extends KeyValue<K> {

	public static final long TTL_KEY_DOES_NOT_EXIST = -2;

	public static boolean isPositiveTtl(Long ttl) {
		return ttl != null && ttl > 0;
	}

	/**
	 * Expiration POSIX time in milliseconds for this key.
	 *
	 */
	private Long ttl;

	public Long getTtl() {
		return ttl;
	}

	public void setTtl(Long ttl) {
		this.ttl = ttl;
	}

	public boolean isInexistent() {
		return ttl != null && ttl == TTL_KEY_DOES_NOT_EXIST;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(ttl);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyTtlValue<?> other = (KeyTtlValue<?>) obj;
		return Objects.equals(ttl, other.ttl);
	}

	public boolean hasTtl() {
		return isPositiveTtl(ttl);
	}

}
