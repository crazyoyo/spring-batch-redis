package com.redis.spring.batch.generator;

import java.util.List;

import com.redis.spring.batch.DataStructure.Type;

public class ListGeneratorItemReader extends CollectionGeneratorItemReader<List<String>> {

	public ListGeneratorItemReader() {
		super(Type.LIST);
	}

	@Override
	protected List<String> value() {
		return members();
	}

}
