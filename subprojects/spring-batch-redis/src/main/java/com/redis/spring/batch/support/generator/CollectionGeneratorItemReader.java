package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.generator.Generator.DataType;

import lombok.Data;
import lombok.EqualsAndHashCode;

public abstract class CollectionGeneratorItemReader<T> extends DataStructureGeneratorItemReader<T> {

	private CollectionOptions options;

	protected CollectionGeneratorItemReader(DataType type, CollectionOptions options) {
		super(type, options);
		this.options = options;
	}

	protected List<String> members() {
		List<String> members = new ArrayList<>();
		for (int index = 0; index < random(options.getCardinality()); index++) {
			members.add("member:" + index);
		}
		return members;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class CollectionOptions extends DataStructureOptions {

		private Range<Integer> cardinality;

		public static CollectionOptionsBuilder<?> builder() {
			return new CollectionOptionsBuilder<>();
		}

		public static class CollectionOptionsBuilder<B extends CollectionOptionsBuilder<B>> extends DataStructureOptionsBuilder<B> {

			public static final int DEFAULT_CARDINALITY_MIN = 10;
			public static final int DEFAULT_CARDINALITY_MAX = 10;

			private Range<Integer> cardinality = Range.between(DEFAULT_CARDINALITY_MIN, DEFAULT_CARDINALITY_MAX);

			@SuppressWarnings("unchecked")
			public B cardinality(Range<Integer> cardinality) {
				this.cardinality = cardinality;
				return (B) this;
			}

			public CollectionOptions build() {
				CollectionOptions options = new CollectionOptions();
				set(options);
				return options;
			}

			@Override
			protected void set(DataStructureOptions options) {
				super.set(options);
				((CollectionOptions) options).setCardinality(cardinality);
			}

		}

	}
}
