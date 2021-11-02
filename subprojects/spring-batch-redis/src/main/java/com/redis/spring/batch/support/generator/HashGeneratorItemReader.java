package com.redis.spring.batch.support.generator;

import java.util.Map;

import com.redis.spring.batch.support.DataStructure.Type;

public class HashGeneratorItemReader extends DataStructureGeneratorItemReader<Map<String, String>> {

	public HashGeneratorItemReader() {
		super(Type.HASH);
	}

	@Override
	protected Map<String, String> value() {
		return map();
	}

}
