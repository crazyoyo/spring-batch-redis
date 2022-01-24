package com.redis.spring.batch.support.generator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.DataStructure.Type;

public class StringGeneratorItemReader extends DataStructureGeneratorItemReader<String> {

	protected Range<Integer> valueSize = Generator.DEFAULT_STRING_VALUE_SIZE;

	public StringGeneratorItemReader() {
		super(Type.STRING);
	}

	public void setValueSize(Range<Integer> valueSize) {
		this.valueSize = valueSize;
	}

	@Override
	protected String value() {
		return RandomStringUtils.randomAscii(valueSize.getMinimum(), valueSize.getMaximum());
	}

}
