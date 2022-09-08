package com.redis.spring.batch.common;

import java.util.Objects;

public class KeyTtlValue<K> extends KeyValue<K> {

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

	public boolean hasTtl() {
		return ttl != null && ttl > 0;
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

}
