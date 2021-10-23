package com.redis.spring.batch.support.generator;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.generator.Generator.DataType;

import io.lettuce.core.ScoredValue;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

public class ZsetGeneratorItemReader extends CollectionGeneratorItemReader<Collection<ScoredValue<String>>> {

	private Options options;

	public ZsetGeneratorItemReader(Options options) {
		super(options.getCollectionOptions(), DataType.ZSET);
		this.options = options;
	}

	@Override
	protected Collection<ScoredValue<String>> value() {
		return members().stream().map(m -> ScoredValue.just(score(), m)).collect(Collectors.toList());
	}

	private double score() {
		double interval = options.getScore().getMaximum() - options.getScore().getMinimum();
		if (interval == 0) {
			return options.getScore().getMinimum();
		}
		return options.getScore().getMinimum() + random.nextDouble() * interval;
	}

	@Data
	@Builder
	public static class Options {

		public static final double DEFAULT_SCORE_MIN = 0;
		public static final double DEFAULT_SCORE_MAX = 100;

		@Default
		private CollectionGeneratorItemReader.Options collectionOptions = CollectionGeneratorItemReader.Options
				.builder().build();
		@Default
		private Range<Double> score = Range.between(DEFAULT_SCORE_MIN, DEFAULT_SCORE_MAX);

		public void to(int max) {
			this.collectionOptions.to(max);
		}

	}

}
