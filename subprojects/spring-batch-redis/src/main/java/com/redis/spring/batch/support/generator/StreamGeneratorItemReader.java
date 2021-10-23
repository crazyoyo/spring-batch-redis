package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.Collection;

import com.redis.spring.batch.support.generator.Generator.DataType;

import io.lettuce.core.StreamMessage;

public class StreamGeneratorItemReader extends DataStructureGeneratorItemReader<Collection<StreamMessage<String, String>>> {

	private CollectionGeneratorItemReader.Options options;

	public StreamGeneratorItemReader(CollectionGeneratorItemReader.Options options) {
		super(options.getDataStructureOptions(), DataType.STREAM);
		this.options = options;
	}

	@Override
	protected Collection<StreamMessage<String, String>> value() {
		String stream = "stream:" + index();
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < random(options.getCardinality()); elementIndex++) {
			messages.add(new StreamMessage<>(stream, null, map()));
		}
		return messages;
	}

}
