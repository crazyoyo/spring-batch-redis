package com.redis.spring.batch.support.generator;

import java.util.Map;

import com.redis.spring.batch.support.DataStructure;

public class HashGeneratorItemReader extends DataStructureGeneratorItemReader<Map<String, String>> {

	public HashGeneratorItemReader() {
		super(DataStructure.HASH);
	}

	@Override
	protected Map<String, String> value() {
		return map();
	}

}
