package com.redis.spring.batch.support.compare;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.redis.spring.batch.support.DataStructure;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class KeyComparison<K> {

	public static final Set<Status> MISMATCHES = new HashSet<>(
			Arrays.asList(Status.MISSING, Status.TYPE, Status.TTL, Status.VALUE));

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
