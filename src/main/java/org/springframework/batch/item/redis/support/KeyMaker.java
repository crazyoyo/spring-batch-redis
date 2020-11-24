package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

public interface KeyMaker<T> extends Converter<T, String> {

	String DEFAULT_SEPARATOR = ":";

	String EMPTY_STRING = "";

	static <T> KeyMakerBuilder<T> builder() {
		return new KeyMakerBuilder<>();
	}

	@Accessors(fluent = true)
	@SuppressWarnings("unchecked")
	public static class KeyMakerBuilder<T> {

		@Setter
		private String separator = DEFAULT_SEPARATOR;

		@Setter
		private String prefix = EMPTY_STRING;

		private Converter<T, Object>[] keyExtractors = new Converter[0];

		public KeyMakerBuilder<T> extractors(Converter<T, Object>... keyExtractors) {
			this.keyExtractors = keyExtractors;
			return this;
		}

		private String getPrefix() {
			if (prefix == null || prefix.isEmpty()) {
				return EMPTY_STRING;
			}
			return prefix + separator;
		}

		public KeyMaker<T> build() {
			if (keyExtractors == null || keyExtractors.length == 0) {
				Assert.isTrue(prefix != null && !prefix.isEmpty(), "No keyspace nor key fields specified");
				return ConstantKeyMaker.<T>builder().key(prefix).build();
			}
			if (keyExtractors.length == 1) {
				return SimpleKeyMaker.<T>builder().prefix(getPrefix()).keyExtractor(keyExtractors[0]).build();
			}
			return CompositeKeyMaker.<T>builder().prefix(getPrefix()).separator(separator).keyExtractors(keyExtractors)
					.build();
		}

	}

	@Builder
	public static class ConstantKeyMaker<T> implements KeyMaker<T> {

		@NonNull
		private final String key;

		@Override
		public String convert(T source) {
			return key;
		}

	}

	@Builder
	public static class SimpleKeyMaker<T> implements KeyMaker<T> {

		@NonNull
		private final String prefix;

		@NonNull
		private final Converter<T, Object> keyExtractor;

		@Override
		public String convert(T source) {
			return prefix + keyExtractor.convert(source);
		}

	}

	@Builder
	public static class CompositeKeyMaker<T> implements KeyMaker<T> {

		@NonNull
		private final String prefix;

		@NonNull
		@Builder.Default
		private final String separator = DEFAULT_SEPARATOR;

		@NonNull
		private final Converter<T, Object>[] keyExtractors;

		@Override
		public String convert(T source) {
			StringBuilder builder = new StringBuilder();
			builder.append(prefix);
			for (int index = 0; index < keyExtractors.length - 1; index++) {
				builder.append(keyExtractors[index].convert(source));
				builder.append(separator);
			}
			builder.append(keyExtractors[keyExtractors.length - 1].convert(source));
			return builder.toString();
		}

	}

}
