package com.redis.spring.batch.support.generator;

import java.util.Map;

import com.redis.spring.batch.support.generator.Generator.DataType;

public class HashGeneratorItemReader extends DataStructureGeneratorItemReader<Map<String, String>> {

	public HashGeneratorItemReader(DataStructureOptions options) {
		super(DataType.HASH, options);
	}

	@Override
	protected Map<String, String> value() {
		return map();
	}

}
