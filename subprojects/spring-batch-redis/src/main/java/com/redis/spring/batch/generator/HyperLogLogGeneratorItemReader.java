package com.redis.spring.batch.generator;

import java.util.HashSet;
import java.util.Set;

import com.redis.spring.batch.DataStructure.Type;

public class HyperLogLogGeneratorItemReader extends CollectionGeneratorItemReader<Set<String>> {

	public HyperLogLogGeneratorItemReader() {
		super(Type.HYPERLOGLOG);
	}

	@Override
	protected Set<String> value() {
		return new HashSet<>(members());
	}

}
