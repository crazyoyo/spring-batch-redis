package com.redis.spring.batch.support.generator;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.generator.Generator.DataType;

import io.lettuce.core.ScoredValue;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class ZsetGeneratorItemReader extends CollectionGeneratorItemReader<Collection<ScoredValue<String>>> {

	private ZsetOptions options;

	public ZsetGeneratorItemReader(ZsetOptions options) {
		super(DataType.ZSET, options);
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
	@EqualsAndHashCode(callSuper = true)
	public static class ZsetOptions extends CollectionOptions {

		private Range<Double> score;

		public static ZsetOptionsBuilder builder() {
			return new ZsetOptionsBuilder();
		}

		public static class ZsetOptionsBuilder extends CollectionOptionsBuilder<ZsetOptionsBuilder> {

			public static final double DEFAULT_SCORE_MIN = 0;
			public static final double DEFAULT_SCORE_MAX = 100;

			private Range<Double> score = Range.between(DEFAULT_SCORE_MIN, DEFAULT_SCORE_MAX);

			public ZsetOptionsBuilder score(Range<Double> score) {
				this.score = score;
				return this;
			}

			public ZsetOptions build() {
				ZsetOptions options = new ZsetOptions();
				set(options);
				return options;
			}

			@Override
			protected void set(DataStructureGeneratorItemReader.DataStructureOptions options) {
				super.set(options);
				((ZsetOptions) options).setScore(score);
			}

		}

	}

}
