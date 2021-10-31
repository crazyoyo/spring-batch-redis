package com.redis.spring.batch.support.generator;

import java.util.List;

import com.redis.spring.batch.support.DataStructure.Type;

public class ListGeneratorItemReader extends CollectionGeneratorItemReader<List<String>> {

	public ListGeneratorItemReader(CollectionOptions options) {
		super(Type.LIST, options);
	}

	@Override
	protected List<String> value() {
		return members();
	}

}
