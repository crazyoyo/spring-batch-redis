package org.springframework.batch.item.redis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public @Data class KeyDump {
	private byte[] key;
	private long ttl;
	private byte[] value;
}