package com.redis.spring.batch.generator;

import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.DataStructure.Type;

public abstract class CollectionGeneratorItemReader<T> extends DataStructureGeneratorItemReader<T> {

	private Range<Long> cardinality = Generator.DEFAULT_COLLECTION_CARDINALITY;

	protected CollectionGeneratorItemReader(Type type) {
		super(type);
	}

	public void setCardinality(Range<Long> cardinality) {
		this.cardinality = cardinality;
	}

	protected List<String> members() {
		List<String> members = new ArrayList<>();
		for (int index = 0; index < cardinality(); index++) {
			members.add("member:" + index);
		}
		return members;
	}

	protected long cardinality() {
		return randomLong(cardinality);
	}

}
