package com.redis.spring.batch.support.generator;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.DataStructure.Type;

import io.lettuce.core.ScoredValue;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class ZsetGeneratorItemReader extends CollectionGeneratorItemReader<Collection<ScoredValue<String>>> {

	private ZsetOptions options;

	public ZsetGeneratorItemReader(ZsetOptions options) {
		super(Type.ZSET, options);
		this.options = options;
	}

	@Override
	protected Collection<ScoredValue<String>> value() {
		return members().stream().map(m -> ScoredValue.just(randomDouble(options.getScore()), m))
				.collect(Collectors.toList());
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class ZsetOptions extends CollectionOptions {

		private Range<Double> score;

		public static ZsetOptionsBuilder builder() {
			return new ZsetOptionsBuilder();
		}

		public static class ZsetOptionsBuilder extends CollectionOptionsBuilder<ZsetOptionsBuilder> {

			private Range<Double> score = Generator.DEFAULT_ZSET_SCORE;

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
