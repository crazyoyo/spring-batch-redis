package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.generator.Generator.DataType;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

public abstract class CollectionGeneratorItemReader<T> extends DataStructureGeneratorItemReader<T> {

	private Options options;

	protected CollectionGeneratorItemReader(Options options, DataType type) {
		super(options.getDataStructureOptions(), type);
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
	@Builder
	public static class Options {

		public static final int DEFAULT_CARDINALITY_MIN = 10;
		public static final int DEFAULT_CARDINALITY_MAX = 10;

		@Default
		private DataStructureGeneratorItemReader.Options dataStructureOptions = DataStructureGeneratorItemReader.Options
				.builder().build();
		@Default
		private Range<Integer> cardinality = Range.between(DEFAULT_CARDINALITY_MIN, DEFAULT_CARDINALITY_MAX);

		public void to(int max) {
			this.dataStructureOptions.to(max);
		}

	}
}
