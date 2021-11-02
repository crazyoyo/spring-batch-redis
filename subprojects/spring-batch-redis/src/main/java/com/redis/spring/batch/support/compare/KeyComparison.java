package com.redis.spring.batch.support.compare;

import com.redis.spring.batch.support.DataStructure;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class KeyComparison<K> {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	private DataStructure<K> source;
	private DataStructure<K> target;
	private Status status;

}
