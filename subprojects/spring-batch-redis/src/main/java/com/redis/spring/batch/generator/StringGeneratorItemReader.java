package com.redis.spring.batch.generator;

import java.util.Random;

import com.redis.spring.batch.DataStructure.Type;

public class StringGeneratorItemReader extends DataStructureGeneratorItemReader<String> {

	private final Random random = new Random();
	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	protected Range<Integer> valueSize = Generator.DEFAULT_STRING_VALUE_SIZE;

	public StringGeneratorItemReader() {
		super(Type.STRING);
	}

	public void setValueSize(Range<Integer> valueSize) {
		this.valueSize = valueSize;
	}

	@Override
	protected String value() {
		int length = valueSize.getMinimum() + random.nextInt((valueSize.getMaximum() - valueSize.getMinimum()) + 1);
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

}
