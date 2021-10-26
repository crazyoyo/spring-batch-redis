package com.redis.spring.batch.support.generator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.generator.Generator.DataType;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class StringGeneratorItemReader extends DataStructureGeneratorItemReader<String> {

	private StringOptions options;

	public StringGeneratorItemReader(StringOptions options) {
		super(DataType.STRING, options);
		this.options = options;
	}

	@Override
	protected String value() {
		return RandomStringUtils.randomAscii(options.getValueSize().getMinimum(), options.getValueSize().getMaximum());
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class StringOptions extends DataStructureOptions {

		private Range<Integer> valueSize;

		public static StringOptionsBuilder builder() {
			return new StringOptionsBuilder();
		}

		public static class StringOptionsBuilder extends DataStructureOptionsBuilder<StringOptionsBuilder> {

			public static final int DEFAULT_MIN_VALUE_SIZE = 100;
			public static final int DEFAULT_MAX_VALUE_SIZE = 100;

			private Range<Integer> valueSize = Range.between(DEFAULT_MIN_VALUE_SIZE, DEFAULT_MAX_VALUE_SIZE);

			public StringOptionsBuilder valueSize(Range<Integer> valueSize) {
				this.valueSize = valueSize;
				return this;
			}

			@Override
			public StringOptions build() {
				StringOptions options = new StringOptions();
				set(options);
				options.setValueSize(valueSize);
				return options;
			}

		}
	}

}
