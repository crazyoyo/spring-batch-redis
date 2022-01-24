package com.redis.spring.batch.support.generator;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Range;

import com.redis.spring.batch.support.DataStructure.Type;

import io.lettuce.core.ScoredValue;

public class ZsetGeneratorItemReader extends CollectionGeneratorItemReader<Collection<ScoredValue<String>>> {

	private Range<Double> score = Generator.DEFAULT_ZSET_SCORE;

	public ZsetGeneratorItemReader() {
		super(Type.ZSET);
	}

	public void setScore(Range<Double> score) {
		this.score = score;
	}

	@Override
	protected Collection<ScoredValue<String>> value() {
		return members().stream().map(m -> ScoredValue.just(randomDouble(score), m)).collect(Collectors.toList());
	}

}
