package com.redis.spring.batch.support.generator;

import java.util.HashSet;
import java.util.Set;

import com.redis.spring.batch.support.generator.Generator.DataType;

public class SetGeneratorItemReader extends CollectionGeneratorItemReader<Set<String>> {

	public SetGeneratorItemReader(CollectionGeneratorItemReader.Options options) {
		super(options, DataType.SET);
	}

	@Override
	protected Set<String> value() {
		return new HashSet<>(members());
	}

}
