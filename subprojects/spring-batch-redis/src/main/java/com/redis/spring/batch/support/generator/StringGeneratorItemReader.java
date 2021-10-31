package com.redis.spring.batch.support.generator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.DataStructure.Type;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class StringGeneratorItemReader extends DataStructureGeneratorItemReader<String> {

	private StringOptions options;

	public StringGeneratorItemReader(StringOptions options) {
		super(Type.STRING, options);
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

			private Range<Integer> valueSize = Generator.DEFAULT_STRING_VALUE_SIZE;

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
