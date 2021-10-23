package com.redis.spring.batch.support.generator;

import com.redis.spring.batch.support.generator.Generator.DataType;

public class StringGeneratorItemReader extends DataStructureGeneratorItemReader<String> {

	public StringGeneratorItemReader(Options options) {
		super(options, DataType.STRING);
	}

	@Override
	protected String value() {
		return value(index());
	}

	public static String value(int index) {
		return "value:" + index;
	}

}
