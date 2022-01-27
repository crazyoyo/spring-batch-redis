package com.redis.spring.batch.generator;

import java.util.Map;

import com.redis.spring.batch.DataStructure.Type;

public class HashGeneratorItemReader extends DataStructureGeneratorItemReader<Map<String, String>> {

	public HashGeneratorItemReader() {
		super(Type.HASH);
	}

	@Override
	protected Map<String, String> value() {
		return map();
	}

}
