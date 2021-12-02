package com.redis.spring.batch.support.generator;

import java.util.List;

import com.redis.spring.batch.support.DataStructure;

public class ListGeneratorItemReader extends CollectionGeneratorItemReader<List<String>> {

	public ListGeneratorItemReader() {
		super(DataStructure.LIST);
	}

	@Override
	protected List<String> value() {
		return members();
	}

}
